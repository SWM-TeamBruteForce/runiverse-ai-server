"""요청 워커 루프: poll → handle → (성공 시) commit."""

import pytest

from runiverse_ai_server.ports import KafkaRecord
from runiverse_ai_server.workers import consume_requests, poll_completions


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


# ── 완료 워커 (S3 주기 확인) ─────────────────────────────


class FakeCompletionFlow:
    def __init__(self, rows, fail_on=()):
        self.rows = list(rows)
        self.checked = []
        self.fail_on = fail_on

    def pending(self):
        return list(self.rows)

    def check(self, row):
        if row in self.fail_on:
            raise RuntimeError("publish failed")
        self.checked.append(row)
        return True


def test_completion_checks_every_pending_row_each_round():
    flow, waits = FakeCompletionFlow(["a", "b"]), []

    rounds = poll_completions(flow, should_stop=stop_after_polls(2), wait=waits.append, interval=10)

    assert (rounds, flow.checked, waits) == (2, ["a", "b", "a", "b"], [10, 10])


def test_completion_error_skips_only_that_row(capsys):
    flow = FakeCompletionFlow(["a", "b"], fail_on={"a"})

    poll_completions(flow, should_stop=stop_after_polls(1), wait=lambda _: None, interval=10)

    assert flow.checked == ["b"]  # a는 다음 확인 때 다시
    assert '"completion_failed"' in capsys.readouterr().out


def test_completion_pending_error_does_not_stop_worker(capsys):
    class BrokenFlow(FakeCompletionFlow):
        def pending(self):
            raise RuntimeError("db down")

    rounds = poll_completions(BrokenFlow([]), should_stop=stop_after_polls(2), wait=lambda _: None, interval=10)

    assert rounds == 2
    assert '"pending_failed"' in capsys.readouterr().out
