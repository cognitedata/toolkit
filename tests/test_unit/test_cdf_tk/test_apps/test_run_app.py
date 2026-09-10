from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps._run import RunApp
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags

# A wide terminal keeps rendered help text on single lines. Some CI runners render
# with color regardless of FORCE_COLOR/isatty (e.g. a cached Console bound to the
# real stdout), which makes Rich split option names like "--host" across style
# codes, so callers must also strip ANSI codes via click.unstyle() before asserting.
_WIDE_TERMINAL = {"COLUMNS": "200"}


class TestRunAppFunctionApp:
    def test_run_help_hides_function_app_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda _flag: False)

        result = CliRunner().invoke(RunApp(), ["--help"], env=_WIDE_TERMINAL)
        output = click.unstyle(result.output)

        assert result.exit_code == 0
        assert "function-app" not in output

    def test_run_help_includes_function_app_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.FUNCTION_APPS)

        result = CliRunner().invoke(RunApp(), ["--help"], env=_WIDE_TERMINAL)
        output = click.unstyle(result.output)

        assert result.exit_code == 0
        assert "function-app" in output

    def test_function_app_help(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.FUNCTION_APPS)

        result = CliRunner().invoke(RunApp(), ["function-app", "--help"], env=_WIDE_TERMINAL)
        output = click.unstyle(result.output)

        assert result.exit_code == 0
        assert "Path to the directory containing handler.py" in output
        assert "discovers and prompts" not in output
        assert "--host" in output
        assert "--port" in output
        assert "--reload" in output
        assert "--log-level" in output

    def test_function_app_requires_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.FUNCTION_APPS)

        result = CliRunner().invoke(RunApp(), ["function-app"], env=_WIDE_TERMINAL)
        output = click.unstyle(result.output)

        assert result.exit_code != 0
        assert "Missing argument" in output

    def test_function_app_forwards_options(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.FUNCTION_APPS)
        command = MagicMock()
        command.run.side_effect = lambda callback: callback()

        with patch(
            "cognite_toolkit._cdf_tk.apps._run.ServeFunctionCommand",
            return_value=command,
        ):
            result = CliRunner().invoke(
                RunApp(),
                [
                    "function-app",
                    str(tmp_path),
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8080",
                    "--no-reload",
                    "--log-level",
                    "debug",
                ],
                env=_WIDE_TERMINAL,
            )

        assert result.exit_code == 0
        command.serve.assert_called_once_with(tmp_path, "0.0.0.0", 8080, False, "debug")
