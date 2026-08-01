import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from forecast_orchestrator.config import RetentionConfig
from forecast_orchestrator.errors import OrchestratorValidationError
from forecast_orchestrator.retention import cleanup_grib, cleanup_zarr
from forecast_orchestrator.state import StateStore


def _ready_job(state: StateStore, root: Path, run: str, name: str) -> Path:
    store = root / name
    store.mkdir(parents=True)
    (store / "READY.json").write_text(json.dumps({"status": "ready"}), encoding="utf-8")
    state.start(
        source="gfs",
        profile="full_energy",
        provider="noaa-gfs",
        model="gfs",
        run_utc=run,
    )
    state.update(
        source="gfs",
        profile="full_energy",
        run_utc=run,
        status="ready",
        stage="published",
        store_path=store,
    )
    return store


def test_superseded_zarr_observes_grace_period(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state")
    state.initialize()
    root = tmp_path / "datasets"
    old = _ready_job(state, root, "2026-08-01T06:00:00Z", "old.zarr")
    current = _ready_job(state, root, "2026-08-01T12:00:00Z", "current.zarr")
    state.mark_all_other_ready_superseded(current_store=current)
    config = RetentionConfig(previous_zarr_runs=0, zarr_deletion_grace_minutes=60)

    assert (
        cleanup_zarr(
            state,
            datasets_root=root,
            current_store=current,
            config=config,
            now=datetime.now(UTC) + timedelta(minutes=30),
        )
        == []
    )
    assert old.is_dir()

    deleted = cleanup_zarr(
        state,
        datasets_root=root,
        current_store=current,
        config=config,
        now=datetime.now(UTC) + timedelta(minutes=61),
    )
    assert deleted == [old.resolve()]
    assert not old.exists()


def test_grib_cleanup_refuses_manifest_outside_managed_root(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state")
    state.initialize()
    outside = tmp_path / "outside" / "manifest.json"
    outside.parent.mkdir()
    outside.write_text(
        json.dumps(
            {
                "status": "complete",
                "run_utc": "2026-08-01T12:00:00+00:00",
                "files": [{"name": "test.grib2"}],
            }
        ),
        encoding="utf-8",
    )
    state.start(
        source="gfs",
        profile="full_energy",
        provider="noaa-gfs",
        model="gfs",
        run_utc="2026-08-01T12:00:00Z",
    )
    state.update(
        source="gfs",
        profile="full_energy",
        run_utc="2026-08-01T12:00:00Z",
        status="ready",
        stage="published",
        manifest_path=outside,
    )

    with pytest.raises(OrchestratorValidationError, match="escapes managed root"):
        cleanup_grib(
            state,
            source="gfs",
            profile="full_energy",
            raw_root=tmp_path / "managed-raw",
            config=RetentionConfig(grib_runs_after_publish=0),
        )

    assert outside.is_file()
