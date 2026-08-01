"""Create durable per-run child configurations without mutating operator templates."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from forecast_orchestrator.errors import ConfigurationError
from forecast_orchestrator.schedule import iso_utc


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        document: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ConfigurationError(f"cannot read template {path}: {error}") from error
    if not isinstance(document, dict):
        raise ConfigurationError(f"template root must be an object: {path}")
    return document


def _write_yaml_atomic(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        yaml.safe_dump(document, stream, sort_keys=False, allow_unicode=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def managed_raw_root(template: Path) -> Path:
    document = _read_yaml(template)
    raw_data_dir = Path(str(document.get("data_dir", "data")))
    data_dir = (
        raw_data_dir.resolve()
        if raw_data_dir.is_absolute()
        else (template.resolve().parent / raw_data_dir).resolve()
    )
    return data_dir / "raw"


def build_ingest_job_config(
    template: Path,
    output: Path,
    *,
    provider: str,
    model: str,
    profile: str,
    run_utc: datetime,
) -> tuple[Path, Path]:
    """Write an explicit-run ingest config and return it with its managed raw root."""
    document = _read_yaml(template)
    request = document.get("request")
    if not isinstance(request, dict):
        raise ConfigurationError(f"ingest template has no request object: {template}")
    configured_provider = request.get("provider")
    configured_model = request.get("model")
    if configured_provider not in (None, provider) or configured_model not in (None, model):
        raise ConfigurationError(
            f"ingest template provider/model does not match {provider}/{model}: {template}"
        )
    request.update(
        {
            "provider": provider,
            "model": model,
            "run": iso_utc(run_utc),
            "profile": profile,
            "variables": [],
            "mode": "download",
        }
    )
    data_dir = managed_raw_root(template).parent
    document["data_dir"] = str(data_dir)
    _write_yaml_atomic(output, document)
    return output, data_dir / "raw"


def build_zarr_job_config(
    template: Path,
    output: Path,
    *,
    input_run: Path,
    output_root: Path,
) -> Path:
    document = _read_yaml(template)
    document["input_run"] = str(input_run.resolve())
    document["output_root"] = str(output_root.resolve())
    _write_yaml_atomic(output, document)
    return output
