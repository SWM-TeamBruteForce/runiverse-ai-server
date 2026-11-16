"""요청 워커 루프: poll → handle → (성공 시) commit."""

import pytest

from runiverse_ai_server.ports import KafkaRecord
from runiverse_ai_server.workers import consume_notifications, consume_requests


def rec(offset):
    return KafkaRecord(topic="t", partition=0, offset=offset, value=b"{}")


class FakeSource:
    def __init__(self, records):
        self.records = list(records)
        self.commits = []

    def poll(self, timeout):
        return self.records.pop(0) if self.records else None

    def commit(self, record):
        self.commits.append(record.offset)


class FakeFlow:
    def __init__(self, fail_on=None):
        self.handled = []
        self.fail_on = fail_on

    def handle(self, record):
        if record.offset == self.fail_on:
            raise RuntimeError("db down")
        self.handled.append(record.offset)


def stop_after_polls(n):
    count = iter(range(n + 1))
    return lambda: next(count) >= n


def test_handles_then_commits_each_record():
    source, flow = FakeSource([rec(0), rec(1)]), FakeFlow()

    handled = consume_requests(source, flow, should_stop=stop_after_polls(3))

    assert (handled, flow.handled, source.commits) == (2, [0, 1], [0, 1])


def test_no_commit_mode_leaves_offsets():
    source, flow = FakeSource([rec(0)]), FakeFlow()

    consume_requests(source, flow, commit=False, should_stop=stop_after_polls(2))

    assert (flow.handled, source.commits) == ([0], [])


def test_failure_propagates_without_commit():
    source, flow = FakeSource([rec(0), rec(1), rec(2)]), FakeFlow(fail_on=1)

    with pytest.raises(RuntimeError):  # 워커를 멈춘다 → 재시작하면 커밋 위치(1)부터 다시 받는다
        consume_requests(source, flow, should_stop=stop_after_polls(5))

    assert source.commits == [0]


def test_idle_polls_do_nothing():
    source, flow = FakeSource([]), FakeFlow()

    assert consume_requests(source, flow, should_stop=stop_after_polls(3)) == 0


# ── 완료 워커 ──────────────────────────────────────────


class FakeNotificationQueue:
    def __init__(self, batches):
        self.batches = list(batches)

    def receive(self, wait_seconds):
        return self.batches.pop(0) if self.batches else []


class FakeCompletionFlow:
    def __init__(self, fail_on=()):
        self.handled = []
        self.fail_on = fail_on

    def handle(self, notification):
        if notification in self.fail_on:
            raise RuntimeError("publish failed")
        self.handled.append(notification)


def test_completion_handles_every_notification():
    flow = FakeCompletionFlow()

    handled = consume_notifications(FakeNotificationQueue([["a", "b"], [], ["c"]]), flow,
                                    should_stop=stop_after_polls(3))

    assert (handled, flow.handled) == (3, ["a", "b", "c"])


def test_completion_error_skips_only_that_notification(capsys):
    flow = FakeCompletionFlow(fail_on={"a"})

    handled = consume_notifications(FakeNotificationQueue([["a", "b"]]), flow, should_stop=stop_after_polls(1))

    assert (handled, flow.handled) == (1, ["b"])  # a는 SQS에 남아 다시 온다
    assert '"completion_failed"' in capsys.readouterr().out
