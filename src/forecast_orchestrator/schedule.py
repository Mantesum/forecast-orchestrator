"""Provider-cycle selection independent of publication timing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def candidate_runs(
    cycles_utc: tuple[int, ...],
    *,
    now: datetime | None = None,
    lookback_hours: int = 30,
) -> tuple[datetime, ...]:
    """Return configured UTC cycles newest first within the lookback window."""
    current = now or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    current = current.astimezone(UTC)
    earliest = current - timedelta(hours=lookback_hours)
    first_day = earliest.replace(hour=0, minute=0, second=0, microsecond=0)
    final_day = current.replace(hour=0, minute=0, second=0, microsecond=0)
    candidates: list[datetime] = []
    day = first_day
    while day <= final_day:
        for hour in cycles_utc:
            candidate = day.replace(hour=hour)
            if earliest <= candidate <= current:
                candidates.append(candidate)
        day += timedelta(days=1)
    return tuple(sorted(candidates, reverse=True))


def iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
