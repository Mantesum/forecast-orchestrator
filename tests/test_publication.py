import json
from pathlib import Path

import pytest

from forecast_orchestrator.config import PublicationConfig
from forecast_orchestrator.errors import PublicationError
from forecast_orchestrator.publication import current_store_path, publish


def _ready(store: Path, run: str, dataset_id: str) -> dict[str, object]:
    store.mkdir(parents=True)
    document: dict[str, object] = {
        "status": "ready",
        "provider": "noaa-gfs",
        "model": "gfs",
        "run_utc": run,
        "dataset_id": dataset_id,
    }
    (store / "READY.json").write_text(json.dumps(document), encoding="utf-8")
    return document


def test_publish_writes_relative_authoritative_pointer(tmp_path: Path) -> None:
    config = PublicationConfig(root=tmp_path, symlink=None)
    store = tmp_path / "datasets" / "noaa-gfs" / "gfs" / "run" / "id.zarr"
    ready = _ready(store, "2026-08-01T12:00:00Z", "id")

    pointer, previous = publish(config, store_path=store, ready=ready)

    assert previous is None
    assert pointer["store"] == "datasets/noaa-gfs/gfs/run/id.zarr"
    assert current_store_path(config) == store.resolve()
    assert not (tmp_path / "current.json.part").exists()


def test_publish_refuses_to_replace_newer_run(tmp_path: Path) -> None:
    config = PublicationConfig(root=tmp_path, symlink=None)
    newer = tmp_path / "datasets" / "new.zarr"
    older = tmp_path / "datasets" / "old.zarr"
    publish(
        config,
        store_path=newer,
        ready=_ready(newer, "2026-08-01T12:00:00Z", "new"),
    )

    with pytest.raises(PublicationError, match="older"):
        publish(
            config,
            store_path=older,
            ready=_ready(older, "2026-08-01T06:00:00Z", "old"),
        )
