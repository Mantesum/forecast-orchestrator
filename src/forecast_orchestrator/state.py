"""Transactional local job ledger and single-process lock."""

from __future__ import annotations

import json
import os
import socket
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from forecast_orchestrator.errors import LockError


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class JobRecord:
    source: str
    profile: str
    provider: str
    model: str
    run_utc: str
    status: str
    stage: str
    attempts: int
    manifest_path: str | None
    store_path: str | None
    error: str | None
    superseded_at: str | None
    created_at: str
    updated_at: str


class StateStore:
    """SQLite-backed state with one durable row per selected model cycle."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.path = self.directory / "state.sqlite3"

    def initialize(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    source TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    run_utc TEXT NOT NULL,
                    status TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    manifest_path TEXT,
                    store_path TEXT,
                    error TEXT,
                    superseded_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source, profile, run_utc)
                )
                """
            )
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(jobs)").fetchall()
            }
            if "superseded_at" not in columns:
                connection.execute("ALTER TABLE jobs ADD COLUMN superseded_at TEXT")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def is_ready(self, source: str, profile: str, run_utc: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM jobs WHERE source=? AND profile=? AND run_utc=?",
                (source, profile, run_utc),
            ).fetchone()
        return row is not None and row["status"] == "ready"

    def start(
        self,
        *,
        source: str,
        profile: str,
        provider: str,
        model: str,
        run_utc: str,
    ) -> None:
        timestamp = _now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    source, profile, provider, model, run_utc, status, stage,
                    attempts, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'running', 'planned', 1, ?, ?)
                ON CONFLICT(source, profile, run_utc) DO UPDATE SET
                    provider=excluded.provider,
                    model=excluded.model,
                    status='running',
                    stage='planned',
                    attempts=jobs.attempts + 1,
                    error=NULL,
                    updated_at=excluded.updated_at
                """,
                (source, profile, provider, model, run_utc, timestamp, timestamp),
            )

    def update(
        self,
        *,
        source: str,
        profile: str,
        run_utc: str,
        status: str,
        stage: str,
        manifest_path: Path | None = None,
        store_path: Path | None = None,
        error: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET
                    status=?, stage=?,
                    manifest_path=COALESCE(?, manifest_path),
                    store_path=COALESCE(?, store_path),
                    error=?, updated_at=?
                WHERE source=? AND profile=? AND run_utc=?
                """,
                (
                    status,
                    stage,
                    str(manifest_path) if manifest_path else None,
                    str(store_path) if store_path else None,
                    error,
                    _now(),
                    source,
                    profile,
                    run_utc,
                ),
            )

    def jobs(self, *, status: str | None = None) -> list[JobRecord]:
        query = "SELECT * FROM jobs"
        parameters: tuple[str, ...] = ()
        if status is not None:
            query += " WHERE status=?"
            parameters = (status,)
        query += " ORDER BY run_utc DESC"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [JobRecord(**dict(row)) for row in rows]

    def mark_superseded(self, store_path: Path) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET superseded_at=COALESCE(superseded_at, ?), updated_at=?
                WHERE store_path=? AND status='ready'
                """,
                (_now(), _now(), str(store_path.resolve())),
            )

    def mark_all_other_ready_superseded(self, *, current_store: Path) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET superseded_at=COALESCE(superseded_at, ?), updated_at=?
                WHERE status='ready' AND store_path IS NOT NULL AND store_path<>?
                """,
                (_now(), _now(), str(current_store.resolve())),
            )


class ProcessLock:
    """Portable exclusive lock file with conservative stale-lock recovery."""

    def __init__(self, directory: Path) -> None:
        self.path = directory.resolve() / "orchestrator.lock"
        self.acquired = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "created_at": _now(),
            }
        ).encode("utf-8")
        for attempt in range(2):
            try:
                descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError as error:
                if attempt == 0 and self._remove_if_stale():
                    continue
                raise LockError(f"another orchestrator process holds {self.path}") from error
            try:
                os.write(descriptor, payload)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self.acquired = True
            return

    def _remove_if_stale(self) -> bool:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            pid = int(raw["pid"])
            hostname = str(raw["hostname"])
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            return False
        if hostname != socket.gethostname():
            return False
        # On POSIX, signal 0 only checks whether a process exists. On Windows,
        # os.kill delegates most signals to TerminateProcess; never use it as a
        # liveness probe there. Production targets Ubuntu, while Windows keeps
        # the conservative behavior of requiring manual stale-lock removal.
        if os.name == "nt":
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            self.path.unlink(missing_ok=True)
            return True
        except (PermissionError, OSError):
            return False
        return False

    def release(self) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)
            self.acquired = False

    def __enter__(self) -> ProcessLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


@contextmanager
def locked(directory: Path) -> Iterator[None]:
    with ProcessLock(directory):
        yield
