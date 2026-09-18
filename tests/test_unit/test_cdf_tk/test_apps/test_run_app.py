from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps._run import RunApp
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags


@pytest.mark.parametrize(
    ("enabled", "arguments", "expected"),
    [
        (False, ["--help"], "function-app"),
        (True, ["function-app", "--help"], "Path to the directory containing handler.py"),
    ],
)
def test_function_app_visibility(
    monkeypatch: pytest.MonkeyPatch, enabled: bool, arguments: list[str], expected: str
) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: enabled and flag is Flags.FUNCTION_APPS)

    result = CliRunner().invoke(RunApp(), arguments, env={"COLUMNS": "200"})

    assert result.exit_code == 0
    assert (expected in click.unstyle(result.output)) is enabled


def test_function_app_forwards_options(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.FUNCTION_APPS)
    command = MagicMock()
    command.run.side_effect = lambda callback: callback()

    with patch("cognite_toolkit._cdf_tk.apps._run.RunFunctionAppCommand", return_value=command):
        result = CliRunner().invoke(
            RunApp(),
            [
                "function-app",
                str(tmp_path),
                "--host",
                "0.0.0.0",
                "--port",
                "8080",
                "--log-level",
                "debug",
            ],
        )

    assert result.exit_code == 0
    command.run_function_app.assert_called_once_with(tmp_path, "0.0.0.0", 8080, "debug")
