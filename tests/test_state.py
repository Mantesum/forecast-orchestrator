from pathlib import Path

import pytest

from forecast_orchestrator.errors import LockError
from forecast_orchestrator.state import ProcessLock, StateStore


def test_state_records_attempts_and_ready_job(tmp_path: Path) -> None:
    state = StateStore(tmp_path)
    state.initialize()
    values = {
        "source": "gfs",
        "profile": "full_energy",
        "provider": "noaa-gfs",
        "model": "gfs",
        "run_utc": "2026-08-01T12:00:00Z",
    }
    state.start(**values)
    state.start(**values)
    state.update(
        source="gfs",
        profile="full_energy",
        run_utc=values["run_utc"],
        status="ready",
        stage="published",
        store_path=tmp_path / "store.zarr",
    )

    job = state.jobs()[0]
    assert job.attempts == 2
    assert job.status == "ready"
    assert state.is_ready("gfs", "full_energy", values["run_utc"])


def test_process_lock_rejects_overlap(tmp_path: Path) -> None:
    first = ProcessLock(tmp_path)
    second = ProcessLock(tmp_path)
    first.acquire()
    try:
        with pytest.raises(LockError):
            second.acquire()
    finally:
        first.release()
