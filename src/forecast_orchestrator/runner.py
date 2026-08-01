"""Subprocess boundary for the two independently installed applications."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from threading import Thread
from typing import Any

from forecast_orchestrator.errors import DependencyError, OrchestratorError


@dataclass(frozen=True)
class CommandResult:
    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def combined_output(self) -> str:
        return "\n".join(part for part in (self.stderr.strip(), self.stdout.strip()) if part)


class CommandFailure(OrchestratorError):
    def __init__(self, message: str, result: CommandResult) -> None:
        super().__init__(message)
        self.result = result


class CommandRunner:
    def require(self, executable: str) -> str:
        candidate = Path(executable)
        if candidate.is_absolute():
            if candidate.is_file():
                return str(candidate)
            raise DependencyError(f"executable does not exist: {candidate}")
        resolved = shutil.which(executable)
        if resolved is None:
            raise DependencyError(f"executable is not available on PATH: {executable}")
        return resolved

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        error_type: type[CommandFailure] = CommandFailure,
    ) -> CommandResult:
        try:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except FileNotFoundError as error:
            raise DependencyError(f"executable is unavailable: {args[0]}") from error
        stdout_stream = process.stdout
        stderr_stream = process.stderr
        assert stdout_stream is not None
        assert stderr_stream is not None
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        def drain_stdout() -> None:
            for line in stdout_stream:
                stdout_parts.append(line)

        def drain_stderr() -> None:
            for line in stderr_stream:
                stderr_parts.append(line)
                print(line, end="", file=sys.stderr, flush=True)

        stdout_thread = Thread(target=drain_stdout, name="child-stdout", daemon=True)
        stderr_thread = Thread(target=drain_stderr, name="child-stderr", daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        timeout_error: subprocess.TimeoutExpired | None = None
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            timeout_error = error
            process.kill()
            returncode = process.wait()
        stdout_thread.join()
        stderr_thread.join()
        result = CommandResult(
            tuple(args), returncode, "".join(stdout_parts), "".join(stderr_parts)
        )
        if timeout_error is not None:
            raise error_type(
                f"command timed out after {timeout}s: {args[0]}", result
            ) from timeout_error
        if returncode != 0:
            detail = result.combined_output[-4000:] or f"exit code {returncode}"
            raise error_type(f"command failed: {detail}", result)
        return result

    def run_json(
        self,
        args: list[str],
        *,
        timeout: int,
        error_type: type[CommandFailure] = CommandFailure,
    ) -> dict[str, Any]:
        result = self.run(args, timeout=timeout, error_type=error_type)
        try:
            document: Any = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise error_type("command did not return valid JSON", result) from error
        if not isinstance(document, dict):
            raise error_type("command JSON root is not an object", result)
        return document
