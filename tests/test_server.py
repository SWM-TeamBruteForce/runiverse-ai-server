"""Server: 워커 스레드 2개 실행·중지. 요청 워커 오류는 서버 전체를 멈춘다."""

import threading
import time

from runiverse_ai_server.ports import KafkaRecord
from runiverse_ai_server.server import Server


class Source:
    def __init__(self, records=()):
        self.records = list(records)
        self.commits = []
        self.closed = False

    def poll(self, timeout):
        if self.records:
            return self.records.pop(0)
        time.sleep(0.01)
        return None

    def commit(self, record):
        self.commits.append(record.offset)

    def close(self):
        self.closed = True


class Queue:
    def __init__(self, batches=()):
        self.batches = list(batches)

    def receive(self, wait_seconds):
        if self.batches:
            return self.batches.pop(0)
        time.sleep(0.01)
        return []


class Flow:
    def __init__(self, fail=False):
        self.handled = []
        self.fail = fail
        self.seen = threading.Event()

    def handle(self, item):
        self.seen.set()
        if self.fail:
            raise RuntimeError("db down")
        self.handled.append(item)


class Publisher:
    def __init__(self):
        self.flushed = False

    def close(self):
        self.flushed = True


def rec(offset):
    return KafkaRecord(topic="t", partition=0, offset=offset, value=b"{}")


def make(request_flow=None, completion_flow=None, records=(rec(0),), batches=(["n1"],)):
    parts = {"source": Source(records), "request_flow": request_flow or Flow(), "queue": Queue(batches),
             "completion_flow": completion_flow or Flow(), "publisher": Publisher()}
    return Server(parts["source"], parts["request_flow"], parts["queue"], parts["completion_flow"],
                  parts["publisher"], poll_timeout=0.01, wait_seconds=0), parts


def test_runs_both_workers_until_stopped():
    server, parts = make()
    server.start()
    parts["request_flow"].seen.wait(2)
    parts["completion_flow"].seen.wait(2)

    server.stop()

    assert server.wait(timeout=2) == 0
    assert parts["source"].commits == [0]
    assert parts["completion_flow"].handled == ["n1"]
    assert parts["source"].closed and parts["publisher"].flushed  # 종료 시 정리


def test_request_worker_error_stops_server():
    server, parts = make(request_flow=Flow(fail=True))
    server.start()

    assert server.wait(timeout=2) == 1  # 비정상 종료 → 재시작하면 커밋 위치부터 다시
    assert parts["source"].commits == []
    assert parts["source"].closed


def test_completion_error_keeps_server_running():
    server, parts = make(completion_flow=Flow(fail=True))
    server.start()
    parts["completion_flow"].seen.wait(2)
    time.sleep(0.05)

    assert server.running
    server.stop()
    assert server.wait(timeout=2) == 0
