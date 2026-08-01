"""Manifest-linked cleanup constrained to explicitly managed roots."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from forecast_orchestrator.config import RetentionConfig
from forecast_orchestrator.errors import RetentionError
from forecast_orchestrator.integrity import ensure_within, parse_datetime, read_json
from forecast_orchestrator.state import JobRecord, StateStore


def _delete_grib_run(job: JobRecord, raw_root: Path) -> Path | None:
    if not job.manifest_path:
        return None
    manifest = ensure_within(Path(job.manifest_path), raw_root, label="retained manifest")
    if manifest.name != "manifest.json" or not manifest.is_file():
        return None
    document = read_json(manifest)
    if document.get("status") != "complete" or parse_datetime(
        document.get("run_utc"), field="manifest.run_utc"
    ) != parse_datetime(job.run_utc, field="job.run_utc"):
        raise RetentionError(f"refusing to delete unverified GRIB run: {manifest.parent}")
    shutil.rmtree(manifest.parent)
    return manifest.parent


def cleanup_grib(
    state: StateStore,
    *,
    source: str,
    profile: str,
    raw_root: Path,
    config: RetentionConfig,
) -> list[Path]:
    jobs = [
        job
        for job in state.jobs(status="ready")
        if job.source == source and job.profile == profile and job.manifest_path
    ]
    deleted: list[Path] = []
    for job in jobs[config.grib_runs_after_publish :]:
        if path := _delete_grib_run(job, raw_root):
            deleted.append(path)
    return deleted


def cleanup_zarr(
    state: StateStore,
    *,
    datasets_root: Path,
    current_store: Path,
    config: RetentionConfig,
    now: datetime | None = None,
) -> list[Path]:
    current = current_store.resolve()
    threshold = (now or datetime.now(UTC)) - timedelta(minutes=config.zarr_deletion_grace_minutes)
    candidates = [
        job
        for job in state.jobs(status="ready")
        if job.store_path and Path(job.store_path).resolve() != current and job.superseded_at
    ]
    keep = config.previous_zarr_runs
    deleted: list[Path] = []
    for index, job in enumerate(candidates):
        if index < keep:
            continue
        try:
            superseded = datetime.fromisoformat(str(job.superseded_at).replace("Z", "+00:00"))
        except ValueError as error:
            raise RetentionError(f"invalid superseded timestamp for {job.store_path}") from error
        if superseded.astimezone(UTC) > threshold:
            continue
        store = ensure_within(Path(str(job.store_path)), datasets_root, label="retained Zarr")
        ready_path = store / "READY.json"
        if not ready_path.is_file() or read_json(ready_path).get("status") != "ready":
            raise RetentionError(f"refusing to delete Zarr without valid READY: {store}")
        shutil.rmtree(store)
        state.update(
            source=job.source,
            profile=job.profile,
            run_utc=job.run_utc,
            status="deleted",
            stage="retained_cleanup",
        )
        deleted.append(store)
    return deleted


def cleanup_staging(
    datasets_root: Path,
    *,
    config: RetentionConfig,
    now: datetime | None = None,
) -> list[Path]:
    staging = ensure_within(datasets_root / ".staging", datasets_root, label="staging root")
    if not staging.exists():
        return []
    threshold = (now or datetime.now(UTC)).timestamp() - config.failed_staging_ttl_hours * 3600
    deleted: list[Path] = []
    for path in staging.glob("*.zarr"):
        if not path.is_dir() or path.stat().st_mtime > threshold:
            continue
        metadata_path = path / "zarr.json"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        attributes = metadata.get("attributes") if isinstance(metadata, dict) else None
        if not isinstance(attributes, dict):
            continue
        if attributes.get("conversion_software") != "forecast-zarr-processor":
            continue
        shutil.rmtree(path)
        deleted.append(path)
    return deleted
