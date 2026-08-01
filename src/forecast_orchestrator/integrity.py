"""Manifest, READY marker, and current pointer integrity checks."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from forecast_orchestrator.errors import OrchestratorValidationError


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        document: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise OrchestratorValidationError(f"invalid JSON document {path}: {error}") from error
    if not isinstance(document, dict):
        raise OrchestratorValidationError(f"JSON root is not an object: {path}")
    return document


def parse_datetime(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise OrchestratorValidationError(f"{field} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise OrchestratorValidationError(f"invalid {field}: {value}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OrchestratorValidationError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def ensure_within(path: Path, root: Path, *, label: str) -> Path:
    resolved = path.resolve()
    resolved_root = root.resolve()
    if not resolved.is_relative_to(resolved_root):
        raise OrchestratorValidationError(
            f"{label} escapes managed root {resolved_root}: {resolved}"
        )
    return resolved


def validate_source_manifest(
    manifest_path: Path,
    *,
    raw_root: Path,
    provider: str,
    model: str,
    run_utc: datetime,
) -> dict[str, Any]:
    manifest_path = ensure_within(manifest_path, raw_root, label="manifest")
    if manifest_path.name != "manifest.json" or not manifest_path.is_file():
        raise OrchestratorValidationError(f"completed manifest is missing: {manifest_path}")
    document = read_json(manifest_path)
    if document.get("status") != "complete":
        raise OrchestratorValidationError("source manifest status is not complete")
    if document.get("provider") != provider or document.get("model") != model:
        raise OrchestratorValidationError("source manifest provider/model does not match the job")
    if parse_datetime(document.get("run_utc"), field="manifest.run_utc") != run_utc.astimezone(UTC):
        raise OrchestratorValidationError("source manifest run_utc does not match the job")
    files = document.get("files")
    if not isinstance(files, list) or not files:
        raise OrchestratorValidationError("source manifest has no artifacts")
    return document


def validate_ready_store(
    store_path: Path,
    *,
    datasets_root: Path,
    manifest_path: Path,
    provider: str,
    model: str,
    run_utc: datetime,
) -> dict[str, Any]:
    store_path = ensure_within(store_path, datasets_root, label="Zarr store")
    ready_path = store_path / "READY.json"
    if not store_path.is_dir() or not ready_path.is_file():
        raise OrchestratorValidationError(f"Zarr READY marker is missing: {store_path}")
    ready = read_json(ready_path)
    if ready.get("status") != "ready":
        raise OrchestratorValidationError("READY status is not ready")
    if ready.get("provider") != provider or ready.get("model") != model:
        raise OrchestratorValidationError("READY provider/model does not match the job")
    if parse_datetime(ready.get("run_utc"), field="READY.run_utc") != run_utc.astimezone(UTC):
        raise OrchestratorValidationError("READY run_utc does not match the job")
    if ready.get("input_manifest_sha256") != sha256_file(manifest_path):
        raise OrchestratorValidationError("READY source manifest hash does not match")
    if not isinstance(ready.get("dataset_id"), str) or not ready["dataset_id"]:
        raise OrchestratorValidationError("READY dataset_id is missing")
    variables = ready.get("variables")
    valid_times = ready.get("valid_times")
    if not isinstance(variables, list) or not variables:
        raise OrchestratorValidationError("READY has no variables")
    if not isinstance(valid_times, list) or not valid_times:
        raise OrchestratorValidationError("READY has no valid times")
    return ready
