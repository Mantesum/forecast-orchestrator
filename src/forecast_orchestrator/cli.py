"""Command-line interface intended for operators and systemd."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from forecast_orchestrator.config import load_config
from forecast_orchestrator.errors import OrchestratorError
from forecast_orchestrator.logging import log_event
from forecast_orchestrator.orchestrator import ForecastOrchestrator

app = typer.Typer(
    name="forecast-orchestrator",
    help="Safely run forecast ingestion, Zarr conversion, publication, and retention.",
    no_args_is_help=True,
)


def _orchestrator(path: Path) -> ForecastOrchestrator:
    return ForecastOrchestrator(load_config(path))


def _echo(document: object) -> None:
    typer.echo(json.dumps(document, indent=2, sort_keys=True, default=str))


def _fail(error: OrchestratorError) -> None:
    log_event("command_failed", error=str(error), exit_code=int(error.exit_code))
    raise typer.Exit(code=int(error.exit_code)) from error


@app.command("run-once")
def run_once(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, readable=True)],
) -> None:
    """Process at most one newly published model cycle."""
    try:
        _echo(_orchestrator(config).run_once())
    except OrchestratorError as error:
        _fail(error)


@app.command()
def status(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, readable=True)],
) -> None:
    """Show the current pointer and durable job history."""
    try:
        _echo(_orchestrator(config).status())
    except OrchestratorError as error:
        _fail(error)


@app.command()
def validate(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, readable=True)],
) -> None:
    """Revalidate the current pointer and its complete Zarr store."""
    try:
        _echo(_orchestrator(config).validate_current())
    except OrchestratorError as error:
        _fail(error)


@app.command()
def doctor(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, readable=True)],
) -> None:
    """Check executables, selected templates, state, and publication paths."""
    try:
        _echo(_orchestrator(config).doctor())
    except OrchestratorError as error:
        _fail(error)


@app.command()
def cleanup(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, readable=True)],
) -> None:
    """Apply configured GRIB, Zarr, and failed-staging retention now."""
    try:
        _echo(_orchestrator(config).cleanup())
    except OrchestratorError as error:
        _fail(error)


if __name__ == "__main__":
    app()
