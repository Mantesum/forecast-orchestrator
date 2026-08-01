from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from forecast_orchestrator.config import (
    AppConfig,
    CommandConfig,
    ProfileConfig,
    PublicationConfig,
    RetentionConfig,
    SourceConfig,
    StateConfig,
)
from forecast_orchestrator.errors import ConversionError
from forecast_orchestrator.integrity import sha256_file
from forecast_orchestrator.orchestrator import ForecastOrchestrator
from forecast_orchestrator.publication import publish
from forecast_orchestrator.runner import CommandFailure, CommandResult, CommandRunner


class FakeRunner(CommandRunner):
    def require(self, executable: str) -> str:
        return executable

    def run_json(self, args: list[str], *, timeout: int, **_: object) -> dict[str, Any]:
        del timeout
        executable, command = args[0], args[1]
        if executable == "ingest" and command == "plan":
            config = _yaml_after(args, "--config")
            request = config["request"]
            return {
                "provider": request["provider"],
                "model": request["model"],
                "run_utc": request["run"],
                "files": [{"name": "test.grib2"}],
                "selected_variables": ["air_temperature_2m"],
                "unsupported_variables": [],
            }
        if executable == "zarr" and command == "plan":
            config = _yaml_after(args, "--config")
            manifest = json.loads(
                (Path(config["input_run"]) / "manifest.json").read_text(encoding="utf-8")
            )
            return {
                "provider": manifest["provider"],
                "model": manifest["model"],
                "run_utc": manifest["run_utc"],
                "variables": [{"name": "air_temperature_2m"}],
                "valid_times": [manifest["run_utc"]],
                "budget": {"passes": True},
            }
        if executable == "zarr" and command == "convert":
            config = _yaml_after(args, "--config")
            input_run = Path(config["input_run"])
            manifest_path = input_run / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            run = datetime.fromisoformat(manifest["run_utc"].replace("Z", "+00:00"))
            store = (
                Path(config["output_root"])
                / manifest["provider"]
                / manifest["model"]
                / run.strftime("%Y%m%dT%H%M%SZ")
                / "dataset.zarr"
            )
            store.mkdir(parents=True)
            ready = {
                "status": "ready",
                "provider": manifest["provider"],
                "model": manifest["model"],
                "run_utc": manifest["run_utc"],
                "dataset_id": "dataset",
                "input_manifest_sha256": sha256_file(manifest_path),
                "variables": ["air_temperature_2m"],
                "valid_times": [manifest["run_utc"]],
            }
            (store / "READY.json").write_text(json.dumps(ready), encoding="utf-8")
            return {"status": "ready", "output": str(store)}
        if executable == "zarr" and command == "validate":
            return {"status": "ready", "arrays": 1}
        raise AssertionError(args)

    def run(self, args: list[str], *, timeout: int, **_: object) -> CommandResult:
        del timeout
        assert args[0:2] == ["ingest", "download"]
        config = _yaml_after(args, "--config")
        request = config["request"]
        run = datetime.fromisoformat(request["run"].replace("Z", "+00:00"))
        run_dir = (
            Path(config["data_dir"])
            / "raw"
            / request["provider"]
            / request["model"]
            / run.strftime("%Y%m%dT%H%M%SZ")
            / "request"
        )
        run_dir.mkdir(parents=True)
        artifact = run_dir / "test.grib2"
        artifact.write_bytes(b"GRIB-test")
        manifest = {
            "status": "complete",
            "provider": request["provider"],
            "model": request["model"],
            "run_utc": request["run"],
            "files": [{"name": artifact.name, "size": artifact.stat().st_size}],
        }
        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return CommandResult(tuple(args), 0, f"Complete: {manifest_path}\n", "")


class FailingConversionRunner(FakeRunner):
    def run_json(self, args: list[str], *, timeout: int, **kwargs: object) -> dict[str, Any]:
        if args[0:2] == ["zarr", "convert"]:
            result = CommandResult(tuple(args), 6, "", "conversion failed")
            raise CommandFailure("conversion failed", result)
        return super().run_json(args, timeout=timeout, **kwargs)


def _yaml_after(args: list[str], option: str) -> dict[str, Any]:
    path = Path(args[args.index(option) + 1])
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _config(tmp_path: Path) -> AppConfig:
    ingest_template = tmp_path / "ingest.yaml"
    ingest_template.write_text(
        yaml.safe_dump(
            {
                "request": {
                    "provider": "noaa-gfs",
                    "model": "gfs",
                    "run": "latest_complete",
                    "areas": [
                        {"kind": "bbox", "north": 90, "south": -90, "west": -180, "east": 180}
                    ],
                    "variables": [],
                    "levels": [],
                    "horizon_hours": 24,
                    "profile": "full_energy",
                    "mode": "download",
                },
                "data_dir": str(tmp_path / "ingest-data"),
            }
        ),
        encoding="utf-8",
    )
    zarr_template = tmp_path / "zarr.yaml"
    zarr_template.write_text(
        yaml.safe_dump({"input_run": "replace", "output_root": "replace"}),
        encoding="utf-8",
    )
    return AppConfig(
        active_source="gfs",
        profile="full_energy",
        sources={
            "gfs": SourceConfig(
                provider="noaa-gfs",
                model="gfs",
                profiles={
                    "full_energy": ProfileConfig(
                        ingest_config=ingest_template,
                        zarr_config=zarr_template,
                    )
                },
            )
        },
        publication=PublicationConfig(root=tmp_path / "public", symlink=None),
        retention=RetentionConfig(
            grib_runs_after_publish=0,
            previous_zarr_runs=0,
            zarr_deletion_grace_minutes=60,
        ),
        commands=CommandConfig(ingest_executable="ingest", zarr_executable="zarr"),
        state=StateConfig(directory=tmp_path / "state"),
    )


def test_complete_pipeline_publishes_before_deleting_grib(tmp_path: Path) -> None:
    config = _config(tmp_path)
    orchestrator = ForecastOrchestrator(config, runner=FakeRunner())

    outcome = orchestrator.run_once(now=datetime(2026, 8, 1, 12, 30, tzinfo=UTC))

    assert outcome["status"] == "ready"
    pointer = json.loads((tmp_path / "public" / "current.json").read_text(encoding="utf-8"))
    assert pointer["run_utc"] == "2026-08-01T12:00:00Z"
    assert (tmp_path / "public" / pointer["store"] / "READY.json").is_file()
    assert not Path(str(outcome["manifest"])).exists()
    assert orchestrator.status()["jobs"][0]["status"] == "ready"


def test_conversion_failure_keeps_previous_forecast_and_grib(tmp_path: Path) -> None:
    config = _config(tmp_path)
    old_store = config.datasets_root / "noaa-gfs" / "gfs" / "old" / "old.zarr"
    old_store.mkdir(parents=True)
    old_ready = {
        "status": "ready",
        "provider": "noaa-gfs",
        "model": "gfs",
        "run_utc": "2026-08-01T06:00:00Z",
        "dataset_id": "old",
    }
    (old_store / "READY.json").write_text(json.dumps(old_ready), encoding="utf-8")
    publish(config.publication, store_path=old_store, ready=old_ready)
    orchestrator = ForecastOrchestrator(config, runner=FailingConversionRunner())

    with pytest.raises(ConversionError, match="conversion failed"):
        orchestrator.run_once(now=datetime(2026, 8, 1, 12, 30, tzinfo=UTC))

    pointer = json.loads((config.publication.root / "current.json").read_text(encoding="utf-8"))
    assert pointer["run_utc"] == "2026-08-01T06:00:00Z"
    failed = orchestrator.status()["jobs"][0]
    assert failed["status"] == "failed"
    assert Path(failed["manifest_path"]).is_file()
