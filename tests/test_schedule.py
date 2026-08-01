from datetime import UTC, datetime

from forecast_orchestrator.schedule import candidate_runs


def test_candidate_runs_are_newest_first_and_respect_selected_cycles() -> None:
    result = candidate_runs(
        (0, 12),
        now=datetime(2026, 8, 1, 13, 30, tzinfo=UTC),
        lookback_hours=30,
    )

    assert result == (
        datetime(2026, 8, 1, 12, tzinfo=UTC),
        datetime(2026, 8, 1, 0, tzinfo=UTC),
        datetime(2026, 7, 31, 12, tzinfo=UTC),
    )


def test_candidate_runs_reject_naive_now() -> None:
    try:
        candidate_runs((0, 6), now=datetime(2026, 8, 1, 12), lookback_hours=12)
    except ValueError as error:
        assert "timezone-aware" in str(error)
    else:  # pragma: no cover - assertion aid
        raise AssertionError("naive datetime was accepted")
