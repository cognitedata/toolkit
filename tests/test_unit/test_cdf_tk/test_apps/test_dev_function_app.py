from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps._dev_app import DevApp
from cognite_toolkit._cdf_tk.apps._dev_function_app import DevFunctionApp
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags


class TestDevFunctionApp:
    def test_dev_help_hides_function_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda _flag: False)

        result = CliRunner().invoke(DevApp(), ["--help"])

        assert result.exit_code == 0
        assert "function" not in result.output

    def test_dev_help_includes_function_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.FUNCTION_APPS)

        result = CliRunner().invoke(DevApp(), ["--help"])

        assert result.exit_code == 0
        assert "function" in result.output

    def test_serve_help(self) -> None:
        result = CliRunner().invoke(DevFunctionApp(), ["serve", "--help"])

        assert result.exit_code == 0
        assert "Path to the directory containing handler.py" in result.output
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--reload" in result.output
        assert "--log-level" in result.output

    def test_serve_forwards_options(self, tmp_path: Path) -> None:
        command = MagicMock()
        command.run.side_effect = lambda callback: callback()

        with patch(
            "cognite_toolkit._cdf_tk.apps._dev_function_app.ServeFunctionCommand",
            return_value=command,
        ):
            result = CliRunner().invoke(
                DevFunctionApp(),
                ["serve", str(tmp_path), "--host", "0.0.0.0", "--port", "8080", "--no-reload", "--log-level", "debug"],
            )

        assert result.exit_code == 0
        command.serve.assert_called_once_with(tmp_path, "0.0.0.0", 8080, False, "debug")
