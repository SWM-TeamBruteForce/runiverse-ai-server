from kafka_check import pending_count


def test_pending_from_committed():
    assert pending_count(low=0, high=10, committed=7) == 3


def test_pending_without_commit_counts_from_low():  # auto.offset.reset=earliest
    assert pending_count(low=4, high=10, committed=None) == 6


def test_pending_never_negative_when_commit_below_retention():
    assert pending_count(low=8, high=10, committed=3) == 2
