import sys

import pytest

from forecast_orchestrator.runner import CommandRunner


def test_runner_preserves_json_stdout_and_streams_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = CommandRunner()
    document = runner.run_json(
        [
            sys.executable,
            "-c",
            "import json,sys; print('progress', file=sys.stderr); print(json.dumps({'ok': True}))",
        ],
        timeout=10,
    )

    assert document == {"ok": True}
    assert "progress" in capsys.readouterr().err
