from pathlib import Path

import pytest

from forecast_orchestrator.config import load_config
from forecast_orchestrator.errors import ConfigurationError


def _write_config(path: Path, *, profile: str = "full_energy") -> None:
    path.write_text(
        f"""
schema_version: "1.0"
active_source: gfs
profile: {profile}
sources:
  gfs:
    provider: noaa-gfs
    model: gfs
    cycles_utc: [18, 0, 6, 12]
    profiles:
      full_energy:
        ingest_config: ingest.yaml
        zarr_config: zarr.yaml
publication:
  root: public
  symlink: null
state:
  directory: state
""".strip(),
        encoding="utf-8",
    )


def test_load_config_resolves_paths_and_normalizes_cycles(tmp_path: Path) -> None:
    path = tmp_path / "orchestrator.yaml"
    _write_config(path)

    config = load_config(path)

    assert config.selected_source.cycles_utc == (0, 6, 12, 18)
    assert config.selected_profile.ingest_config == (tmp_path / "ingest.yaml").resolve()
    assert config.publication.root == (tmp_path / "public").resolve()
    assert config.state.directory == (tmp_path / "state").resolve()


def test_active_profile_must_be_configured(tmp_path: Path) -> None:
    path = tmp_path / "orchestrator.yaml"
    _write_config(path, profile="weather")

    with pytest.raises(ConfigurationError, match="not configured"):
        load_config(path)
