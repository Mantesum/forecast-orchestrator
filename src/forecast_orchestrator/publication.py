"""Atomic filesystem/NFS publication of one immutable ready Zarr store."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from forecast_orchestrator.config import PublicationConfig
from forecast_orchestrator.errors import PublicationError
from forecast_orchestrator.integrity import ensure_within, parse_datetime, read_json, sha256_file


def current_pointer(config: PublicationConfig) -> dict[str, Any] | None:
    path = config.root / config.pointer
    if not path.exists():
        return None
    return read_json(path)


def current_store_path(config: PublicationConfig) -> Path | None:
    pointer = current_pointer(config)
    if pointer is None:
        return None
    relative = pointer.get("store")
    if not isinstance(relative, str):
        raise PublicationError("current pointer has no store path")
    path = (config.root / relative).resolve()
    return ensure_within(path, config.root / config.datasets_directory, label="current store")


def _atomic_json(path: Path, document: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".part")
    path.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(document, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _replace_symlink(config: PublicationConfig, store_path: Path) -> None:
    if config.symlink is None:
        return
    link_path = config.root / config.symlink
    if link_path.exists() and not link_path.is_symlink():
        raise PublicationError(f"refusing to replace a non-symlink path: {link_path}")
    temporary = config.root / f".{config.symlink}.part"
    temporary.unlink(missing_ok=True)
    relative_target = os.path.relpath(store_path, start=config.root)
    try:
        os.symlink(relative_target, temporary, target_is_directory=True)
        os.replace(temporary, link_path)
    except OSError as error:
        temporary.unlink(missing_ok=True)
        raise PublicationError(f"cannot publish symlink {link_path}: {error}") from error


def publish(
    config: PublicationConfig,
    *,
    store_path: Path,
    ready: dict[str, Any],
) -> tuple[dict[str, Any], Path | None]:
    """Expose a validated immutable store and return pointer plus previous store."""
    root = config.root.resolve()
    datasets_root = (root / config.datasets_directory).resolve()
    store_path = ensure_within(store_path, datasets_root, label="published store")
    previous = current_pointer(config)
    previous_store = current_store_path(config) if previous is not None else None
    new_run = parse_datetime(ready.get("run_utc"), field="READY.run_utc")
    if previous is not None:
        old_run = parse_datetime(previous.get("run_utc"), field="current.run_utc")
        if old_run > new_run:
            raise PublicationError("refusing to replace current forecast with an older run")

    try:
        relative_store = store_path.relative_to(root).as_posix()
    except ValueError as error:  # guarded by ensure_within; retained as a defensive boundary
        raise PublicationError("published store is outside publication root") from error
    pointer = {
        "schema_version": "1.0",
        "status": "ready",
        "provider": ready["provider"],
        "model": ready["model"],
        "run_utc": ready["run_utc"],
        "dataset_id": ready["dataset_id"],
        "store": relative_store,
        "ready_sha256": sha256_file(store_path / "READY.json"),
        "published_at": datetime.now(UTC).isoformat(),
    }
    root.mkdir(parents=True, exist_ok=True)
    _replace_symlink(config, store_path)
    _atomic_json(root / config.pointer, pointer)
    return pointer, previous_store
