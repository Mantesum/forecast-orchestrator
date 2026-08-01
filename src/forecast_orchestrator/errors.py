"""Stable application errors and exit codes."""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    OK = 0
    CONFIGURATION = 2
    LOCKED = 3
    DEPENDENCY = 4
    INGEST = 5
    CONVERSION = 6
    VALIDATION = 7
    PUBLICATION = 8
    RETENTION = 9


class OrchestratorError(Exception):
    """Base class for expected operational failures."""

    exit_code = ExitCode.CONFIGURATION


class ConfigurationError(OrchestratorError):
    exit_code = ExitCode.CONFIGURATION


class LockError(OrchestratorError):
    exit_code = ExitCode.LOCKED


class DependencyError(OrchestratorError):
    exit_code = ExitCode.DEPENDENCY


class IngestError(OrchestratorError):
    exit_code = ExitCode.INGEST


class ConversionError(OrchestratorError):
    exit_code = ExitCode.CONVERSION


class OrchestratorValidationError(OrchestratorError):
    exit_code = ExitCode.VALIDATION


class PublicationError(OrchestratorError):
    exit_code = ExitCode.PUBLICATION


class RetentionError(OrchestratorError):
    exit_code = ExitCode.RETENTION
