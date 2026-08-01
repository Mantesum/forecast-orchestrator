"""Small structured logging surface suitable for journald."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any


def log_event(event: str, **fields: Any) -> None:
    document = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
        **fields,
    }
    print(json.dumps(document, sort_keys=True, default=str), file=sys.stderr, flush=True)
