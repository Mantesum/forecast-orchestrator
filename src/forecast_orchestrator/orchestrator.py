"""One-run orchestration across discovery, ingest, conversion, publication, and retention."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from forecast_orchestrator.config import AppConfig
from forecast_orchestrator.errors import (
    ConfigurationError,
    ConversionError,
    IngestError,
    OrchestratorValidationError,
)
from forecast_orchestrator.integrity import (
    parse_datetime,
    read_json,
    sha256_file,
    validate_ready_store,
    validate_source_manifest,
)
from forecast_orchestrator.job_config import (
    build_ingest_job_config,
    build_zarr_job_config,
    managed_raw_root,
)
from forecast_orchestrator.logging import log_event
from forecast_orchestrator.publication import current_pointer, current_store_path, publish
from forecast_orchestrator.retention import cleanup_grib, cleanup_staging, cleanup_zarr
from forecast_orchestrator.runner import CommandFailure, CommandRunner
from forecast_orchestrator.schedule import candidate_runs, iso_utc
from forecast_orchestrator.state import StateStore, locked


class ForecastOrchestrator:
    def __init__(self, config: AppConfig, *, runner: CommandRunner | None = None) -> None:
        self.config = config
        self.runner = runner or CommandRunner()
        self.state = StateStore(config.state.directory)

    def run_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        self.state.initialize()
        with locked(self.config.state.directory):
            return self._run_once_locked(now=now)

    def _run_once_locked(self, *, now: datetime | None = None) -> dict[str, Any]:
        ingest = self.runner.require(self.config.commands.ingest_executable)
        zarr = self.runner.require(self.config.commands.zarr_executable)
        source = self.config.selected_source
        current = current_pointer(self.config.publication)
        same_current_product = (
            current is not None
            and current.get("provider") == source.provider
            and current.get("model") == source.model
        )
        current_run = (
            parse_datetime(current.get("run_utc"), field="current.run_utc")
            if same_current_product and current is not None
            else None
        )
        candidates = candidate_runs(
            source.cycles_utc,
            now=now,
            lookback_hours=self.config.schedule.lookback_hours,
        )
        for run_utc in candidates:
            run_id = iso_utc(run_utc)
            if current_run is not None and run_utc <= current_run:
                continue
            if self.state.is_ready(self.config.active_source, self.config.profile, run_id):
                continue
            result = self._try_candidate(
                ingest=ingest,
                zarr=zarr,
                run_utc=run_utc,
            )
            if result is not None:
                return result
        current_store = current_store_path(self.config.publication)
        cleanup: dict[str, Any] | None = None
        if current_store is not None:
            self.state.mark_all_other_ready_superseded(current_store=current_store)
            cleanup = self._cleanup_best_effort(
                raw_root=managed_raw_root(self.config.selected_profile.ingest_config),
                current_store=current_store,
            )
        else:
            self._cleanup_staging_best_effort()
        return {
            "status": "waiting",
            "source": self.config.active_source,
            "profile": self.config.profile,
            "message": "no newer complete forecast run is available",
            "cleanup": cleanup,
        }

    def _try_candidate(
        self,
        *,
        ingest: str,
        zarr: str,
        run_utc: datetime,
    ) -> dict[str, Any] | None:
        source = self.config.selected_source
        profile = self.config.selected_profile
        run_id = iso_utc(run_utc)
        token = run_utc.strftime("%Y%m%dT%H%M%SZ")
        job_dir = self.config.state.directory / "jobs" / self.config.active_source / token
        ingest_job, raw_root = build_ingest_job_config(
            profile.ingest_config,
            job_dir / "ingest.yaml",
            provider=source.provider,
            model=source.model,
            profile=self.config.profile,
            run_utc=run_utc,
        )
        try:
            plan = self.runner.run_json(
                [ingest, "plan", "--config", str(ingest_job), "--json"],
                timeout=self.config.commands.probe_timeout_seconds,
            )
        except CommandFailure as error:
            if _is_not_yet_published(error):
                log_event(
                    "forecast_not_yet_available",
                    source=self.config.active_source,
                    run_utc=run_id,
                )
                return None
            raise IngestError(str(error)) from error
        self._validate_ingest_plan(plan, run_utc)
        unsupported = plan.get("unsupported_variables")
        if unsupported and not profile.allow_unsupported_variables:
            raise ConfigurationError(
                f"source {self.config.active_source} does not support profile "
                f"{self.config.profile}: {unsupported}"
            )

        self.state.start(
            source=self.config.active_source,
            profile=self.config.profile,
            provider=source.provider,
            model=source.model,
            run_utc=run_id,
        )
        log_event(
            "job_started",
            source=self.config.active_source,
            profile=self.config.profile,
            run_utc=run_id,
        )
        try:
            return self._execute_job(
                ingest=ingest,
                zarr=zarr,
                ingest_job=ingest_job,
                raw_root=raw_root,
                job_dir=job_dir,
                run_utc=run_utc,
            )
        except Exception as error:
            self.state.update(
                source=self.config.active_source,
                profile=self.config.profile,
                run_utc=run_id,
                status="failed",
                stage="failed",
                error=str(error)[-4000:],
            )
            log_event("job_failed", run_utc=run_id, error=str(error))
            raise

    def _execute_job(
        self,
        *,
        ingest: str,
        zarr: str,
        ingest_job: Path,
        raw_root: Path,
        job_dir: Path,
        run_utc: datetime,
    ) -> dict[str, Any]:
        source = self.config.selected_source
        run_id = iso_utc(run_utc)
        try:
            download = self.runner.run(
                [ingest, "download", "--config", str(ingest_job)],
                timeout=self.config.commands.download_timeout_seconds,
            )
        except CommandFailure as error:
            raise IngestError(str(error)) from error
        manifest_path = _manifest_from_download_output(download.stdout)
        validate_source_manifest(
            manifest_path,
            raw_root=raw_root,
            provider=source.provider,
            model=source.model,
            run_utc=run_utc,
        )
        self.state.update(
            source=self.config.active_source,
            profile=self.config.profile,
            run_utc=run_id,
            status="running",
            stage="downloaded",
            manifest_path=manifest_path,
        )
        log_event("ingest_complete", run_utc=run_id, manifest=str(manifest_path))

        zarr_job = build_zarr_job_config(
            self.config.selected_profile.zarr_config,
            job_dir / "zarr.yaml",
            input_run=manifest_path.parent,
            output_root=self.config.datasets_root,
        )
        try:
            zarr_plan = self.runner.run_json(
                [zarr, "plan", "--config", str(zarr_job)],
                timeout=self.config.commands.probe_timeout_seconds,
            )
        except CommandFailure as error:
            raise ConversionError(str(error)) from error
        self._validate_zarr_plan(zarr_plan, run_utc)
        try:
            converted = self.runner.run_json(
                [zarr, "convert", "--config", str(zarr_job)],
                timeout=self.config.commands.conversion_timeout_seconds,
            )
        except CommandFailure as error:
            raise ConversionError(str(error)) from error
        if converted.get("status") != "ready" or not isinstance(converted.get("output"), str):
            raise ConversionError("forecast-zarr convert did not return a ready output path")
        store_path = Path(converted["output"]).resolve()
        try:
            self.runner.run_json(
                [zarr, "validate", str(store_path)],
                timeout=self.config.commands.validation_timeout_seconds,
            )
        except CommandFailure as error:
            raise OrchestratorValidationError(str(error)) from error
        ready = validate_ready_store(
            store_path,
            datasets_root=self.config.datasets_root,
            manifest_path=manifest_path,
            provider=source.provider,
            model=source.model,
            run_utc=run_utc,
        )
        self.state.update(
            source=self.config.active_source,
            profile=self.config.profile,
            run_utc=run_id,
            status="running",
            stage="validated",
            store_path=store_path,
        )
        pointer, previous_store = publish(
            self.config.publication,
            store_path=store_path,
            ready=ready,
        )
        self.state.update(
            source=self.config.active_source,
            profile=self.config.profile,
            run_utc=run_id,
            status="ready",
            stage="published",
            manifest_path=manifest_path,
            store_path=store_path,
        )
        if previous_store is not None and previous_store != store_path:
            self.state.mark_superseded(previous_store)
        self.state.mark_all_other_ready_superseded(current_store=store_path)
        cleanup = self._cleanup_best_effort(raw_root=raw_root, current_store=store_path)
        log_event("job_ready", run_utc=run_id, store=str(store_path))
        return {
            "status": "ready",
            "source": self.config.active_source,
            "profile": self.config.profile,
            "run_utc": run_id,
            "manifest": str(manifest_path),
            "store": str(store_path),
            "pointer": pointer,
            "cleanup": cleanup,
        }

    def _validate_ingest_plan(self, plan: dict[str, Any], run_utc: datetime) -> None:
        source = self.config.selected_source
        if plan.get("provider") != source.provider or plan.get("model") != source.model:
            raise IngestError("forecast-ingest plan provider/model mismatch")
        if parse_datetime(plan.get("run_utc"), field="ingest plan run_utc") != run_utc:
            raise IngestError("forecast-ingest planned an unexpected run")
        if not plan.get("files") or not plan.get("selected_variables"):
            raise IngestError("forecast-ingest plan has no files or variables")

    def _validate_zarr_plan(self, plan: dict[str, Any], run_utc: datetime) -> None:
        source = self.config.selected_source
        if plan.get("provider") != source.provider or plan.get("model") != source.model:
            raise ConversionError("forecast-zarr plan provider/model mismatch")
        if parse_datetime(plan.get("run_utc"), field="Zarr plan run_utc") != run_utc:
            raise ConversionError("forecast-zarr planned an unexpected run")
        budget = plan.get("budget")
        if not isinstance(budget, dict) or budget.get("passes") is not True:
            raise ConversionError(f"forecast-zarr budget does not pass: {budget}")
        if not plan.get("variables") or not plan.get("valid_times"):
            raise ConversionError("forecast-zarr plan has no variables or valid times")

    def _cleanup_best_effort(self, *, raw_root: Path, current_store: Path) -> dict[str, Any]:
        result: dict[str, Any] = {"grib": [], "zarr": [], "staging": [], "warnings": []}
        actions: tuple[tuple[str, Callable[[], list[Path]]], ...] = (
            (
                "grib",
                lambda: cleanup_grib(
                    self.state,
                    source=self.config.active_source,
                    profile=self.config.profile,
                    raw_root=raw_root,
                    config=self.config.retention,
                ),
            ),
            (
                "zarr",
                lambda: cleanup_zarr(
                    self.state,
                    datasets_root=self.config.datasets_root,
                    current_store=current_store,
                    config=self.config.retention,
                ),
            ),
            (
                "staging",
                lambda: cleanup_staging(
                    self.config.datasets_root,
                    config=self.config.retention,
                ),
            ),
        )
        for name, action in actions:
            try:
                result[name] = [str(path) for path in action()]
            except Exception as error:  # cleanup must never revoke a ready forecast
                result["warnings"].append(f"{name}: {error}")
                log_event("cleanup_failed", kind=name, error=str(error))
        return result

    def _cleanup_staging_best_effort(self) -> None:
        try:
            cleanup_staging(self.config.datasets_root, config=self.config.retention)
        except Exception as error:
            log_event("cleanup_failed", kind="staging", error=str(error))

    def status(self) -> dict[str, Any]:
        self.state.initialize()
        return {
            "status": "healthy",
            "active_source": self.config.active_source,
            "profile": self.config.profile,
            "current": current_pointer(self.config.publication),
            "jobs": [asdict(job) for job in self.state.jobs()],
        }

    def validate_current(self) -> dict[str, Any]:
        zarr = self.runner.require(self.config.commands.zarr_executable)
        pointer = current_pointer(self.config.publication)
        store = current_store_path(self.config.publication)
        if pointer is None or store is None:
            raise OrchestratorValidationError("no current forecast is published")
        ready = read_json(store / "READY.json")
        for field in ("provider", "model", "run_utc", "dataset_id"):
            if pointer.get(field) != ready.get(field):
                raise OrchestratorValidationError(f"current pointer differs from READY: {field}")
        if pointer.get("ready_sha256") != sha256_file(store / "READY.json"):
            raise OrchestratorValidationError("current READY checksum mismatch")
        try:
            validation = self.runner.run_json(
                [zarr, "validate", str(store)],
                timeout=self.config.commands.validation_timeout_seconds,
            )
        except CommandFailure as error:
            raise OrchestratorValidationError(str(error)) from error
        return {"status": "ready", "pointer": pointer, "validation": validation}

    def cleanup(self) -> dict[str, Any]:
        self.state.initialize()
        with locked(self.config.state.directory):
            store = current_store_path(self.config.publication)
            if store is None:
                raise OrchestratorValidationError("no current forecast is published")
            self.state.mark_all_other_ready_superseded(current_store=store)
            raw_root = managed_raw_root(self.config.selected_profile.ingest_config)
            return self._cleanup_best_effort(raw_root=raw_root, current_store=store)

    def doctor(self) -> dict[str, Any]:
        ingest = self.runner.require(self.config.commands.ingest_executable)
        zarr = self.runner.require(self.config.commands.zarr_executable)
        for template in (
            self.config.selected_profile.ingest_config,
            self.config.selected_profile.zarr_config,
        ):
            if not template.is_file():
                raise ConfigurationError(f"configuration template does not exist: {template}")
        self.state.initialize()
        self.config.datasets_root.mkdir(parents=True, exist_ok=True)
        return {
            "status": "healthy",
            "ingest_executable": ingest,
            "zarr_executable": zarr,
            "state_directory": str(self.config.state.directory),
            "publication_root": str(self.config.publication.root),
            "active_source": self.config.active_source,
            "profile": self.config.profile,
        }


def _is_not_yet_published(error: CommandFailure) -> bool:
    output = error.result.combined_output.lower()
    return "has not been published" in output or "run unavailable" in output


def _manifest_from_download_output(stdout: str) -> Path:
    for line in reversed(stdout.splitlines()):
        if line.startswith("Complete:"):
            value = line.partition(":")[2].strip()
            if value:
                return Path(value).resolve()
    raise IngestError("forecast-ingest download did not report the completed manifest path")
