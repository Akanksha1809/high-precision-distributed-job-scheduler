from app.workers.retry import compute_backoff_seconds


def test_backoff_grows_exponentially_before_cap():
    d1 = compute_backoff_seconds(1, base_delay_seconds=2.0, max_delay_seconds=1000, jitter_seconds=0)
    d2 = compute_backoff_seconds(2, base_delay_seconds=2.0, max_delay_seconds=1000, jitter_seconds=0)
    d3 = compute_backoff_seconds(3, base_delay_seconds=2.0, max_delay_seconds=1000, jitter_seconds=0)
    assert d1 == 2.0
    assert d2 == 4.0
    assert d3 == 8.0


def test_backoff_respects_max_delay_cap():
    d = compute_backoff_seconds(20, base_delay_seconds=2.0, max_delay_seconds=30, jitter_seconds=0)
    assert d == 30.0


def test_jitter_adds_bounded_randomness():
    for _ in range(50):
        d = compute_backoff_seconds(1, base_delay_seconds=2.0, max_delay_seconds=1000, jitter_seconds=5.0)
        assert 2.0 <= d <= 7.0


def test_never_negative():
    d = compute_backoff_seconds(0, base_delay_seconds=2.0, max_delay_seconds=1000, jitter_seconds=0)
    assert d >= 0
