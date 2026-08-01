"""Forecast ingestion and Zarr publication orchestrator."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("forecast-orchestrator")
except PackageNotFoundError:  # pragma: no cover - editable source tree without installation
    __version__ = "0.1.0"

__all__ = ["__version__"]
