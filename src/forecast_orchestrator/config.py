"""Strict, path-aware YAML configuration."""

from __future__ import annotations

from pathlib import Path, PurePath
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forecast_orchestrator.errors import ConfigurationError


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProfileConfig(StrictModel):
    ingest_config: Path
    zarr_config: Path
    allow_unsupported_variables: bool = False


class SourceConfig(StrictModel):
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    cycles_utc: tuple[int, ...] = (0, 6, 12, 18)
    profiles: dict[str, ProfileConfig]

    @field_validator("cycles_utc")
    @classmethod
    def valid_cycles(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if not value:
            raise ValueError("cycles_utc must contain at least one hour")
        if len(value) != len(set(value)):
            raise ValueError("cycles_utc must be unique")
        if any(hour < 0 or hour > 23 for hour in value):
            raise ValueError("cycles_utc values must be between 0 and 23")
        return tuple(sorted(value))


class ScheduleConfig(StrictModel):
    poll_interval_minutes: int = Field(default=10, ge=1, le=60)
    maximum_start_delay_minutes: int = Field(default=30, ge=1, le=180)
    lookback_hours: int = Field(default=30, ge=6, le=168)

    @model_validator(mode="after")
    def poll_meets_start_delay_objective(self) -> ScheduleConfig:
        if self.poll_interval_minutes > self.maximum_start_delay_minutes:
            raise ValueError("poll interval exceeds maximum_start_delay_minutes")
        return self


class PublicationConfig(StrictModel):
    backend: Literal["filesystem"] = "filesystem"
    root: Path = Path("/srv/forecast-public")
    datasets_directory: str = "datasets"
    pointer: str = "current.json"
    symlink: str | None = "current.zarr"

    @field_validator("datasets_directory", "pointer")
    @classmethod
    def simple_relative_name(cls, value: str) -> str:
        path = PurePath(value)
        if path.is_absolute() or len(path.parts) != 1 or value in {"", ".", ".."}:
            raise ValueError("publication names must be one safe relative path component")
        return value

    @field_validator("symlink")
    @classmethod
    def simple_optional_relative_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = PurePath(value)
        if path.is_absolute() or len(path.parts) != 1 or value in {"", ".", ".."}:
            raise ValueError("publication symlink must be one safe relative path component")
        return value


class RetentionConfig(StrictModel):
    grib_runs_after_publish: int = Field(default=0, ge=0, le=100)
    previous_zarr_runs: int = Field(default=0, ge=0, le=100)
    zarr_deletion_grace_minutes: int = Field(default=60, ge=0, le=10080)
    failed_staging_ttl_hours: int = Field(default=72, ge=1, le=2160)


class CommandConfig(StrictModel):
    ingest_executable: str = "forecast-ingest"
    zarr_executable: str = "forecast-zarr"
    # Building an inventory for a global full_energy GRIB run can take several
    # minutes, even though the NOAA availability probe itself is fast.
    probe_timeout_seconds: int = Field(default=1800, ge=10, le=3600)
    download_timeout_seconds: int = Field(default=21600, ge=60, le=86400)
    conversion_timeout_seconds: int = Field(default=21600, ge=60, le=86400)
    validation_timeout_seconds: int = Field(default=3600, ge=60, le=21600)


class StateConfig(StrictModel):
    directory: Path = Path("/var/lib/forecast-orchestrator")


class AppConfig(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    active_source: str
    profile: str = "full_energy"
    sources: dict[str, SourceConfig]
    schedule: ScheduleConfig = ScheduleConfig()
    publication: PublicationConfig = PublicationConfig()
    retention: RetentionConfig = RetentionConfig()
    commands: CommandConfig = CommandConfig()
    state: StateConfig = StateConfig()

    @model_validator(mode="after")
    def active_selection_exists(self) -> AppConfig:
        source = self.sources.get(self.active_source)
        if source is None:
            raise ValueError(f"active_source is absent from sources: {self.active_source}")
        if self.profile not in source.profiles:
            raise ValueError(
                f"profile {self.profile!r} is not configured for source {self.active_source!r}"
            )
        return self

    @property
    def selected_source(self) -> SourceConfig:
        return self.sources[self.active_source]

    @property
    def selected_profile(self) -> ProfileConfig:
        return self.selected_source.profiles[self.profile]

    @property
    def datasets_root(self) -> Path:
        return self.publication.root / self.publication.datasets_directory


def _resolved(path: Path, base: Path) -> Path:
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def load_config(path: Path) -> AppConfig:
    """Load YAML and resolve every local path relative to that YAML file."""
    try:
        raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("configuration root must be an object")
        config = AppConfig.model_validate(raw)
    except (OSError, ValueError, yaml.YAMLError) as error:
        raise ConfigurationError(f"invalid configuration {path}: {error}") from error

    base = path.resolve().parent
    sources: dict[str, SourceConfig] = {}
    for source_name, source in config.sources.items():
        profiles = {
            profile_name: profile.model_copy(
                update={
                    "ingest_config": _resolved(profile.ingest_config, base),
                    "zarr_config": _resolved(profile.zarr_config, base),
                }
            )
            for profile_name, profile in source.profiles.items()
        }
        sources[source_name] = source.model_copy(update={"profiles": profiles})

    return config.model_copy(
        update={
            "sources": sources,
            "publication": config.publication.model_copy(
                update={"root": _resolved(config.publication.root, base)}
            ),
            "state": config.state.model_copy(
                update={"directory": _resolved(config.state.directory, base)}
            ),
        }
    )
