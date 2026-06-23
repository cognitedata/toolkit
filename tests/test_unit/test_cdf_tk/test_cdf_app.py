from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
import typer.main

from cognite_toolkit import _cdf
from cognite_toolkit._cdf_tk import plugins
from cognite_toolkit._cdf_tk.apps._core_app import CoreApp, StatusOutputFormat
from cognite_toolkit._cdf_tk.plugins import Plugins


class TestNoSuchCommandPattern:
    @pytest.mark.parametrize(
        "traceback_text",
        [
            # Newer Click/Typer raises the NoSuchCommand subclass.
            "click.exceptions.NoSuchCommand: No such command 'data'.",
            # Older Click raised the base UsageError.
            "click.exceptions.UsageError: No such command 'data'.",
        ],
    )
    def test_matches_known_traceback_formats(self, traceback_text: str) -> None:
        match = _cdf.NO_SUCH_COMMAND_PATTERN.search(traceback_text)
        assert match is not None
        assert match.group(1) == "data"


class TestStatusCommand:
    def test_status_accepts_json_option(self) -> None:
        command = typer.main.get_command(_cdf._app)
        status = command.commands["status"]

        option_names = {name for param in status.params for name in param.opts}

        assert "--json" in option_names

    def test_status_json_suppresses_deprecation_warning(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        env_vars = MagicMock()
        env_vars.get_client.return_value = MagicMock()
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.EnvironmentVariables.create_from_environment",
            lambda: env_vars,
        )
        printed_warnings: list[str] = []
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.ToolkitDeprecationWarning.print_warning",
            lambda self: printed_warnings.append(str(self)),
        )
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.StatusCommand.run",
            lambda self, execute: execute(),
        )
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.StatusCommand.execute",
            lambda self, **kwargs: None,
        )

        CoreApp().status(
            ctx=MagicMock(),
            organization_dir=tmp_path,
            selected=None,
            config_yaml=None,
            build_env_name="dev",
            output_format=StatusOutputFormat.json,
            json_output=False,
            verbose=False,
        )

        assert printed_warnings == []

    def test_status_continues_without_authenticated_client(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        env_vars = MagicMock()
        env_vars.get_client.side_effect = RuntimeError("missing credentials")
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.EnvironmentVariables.create_from_environment",
            lambda: env_vars,
        )
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.StatusCommand.run",
            lambda self, execute: execute(),
        )
        clients: list[object | None] = []

        def capture_execute(self: object, **kwargs: object) -> None:
            clients.append(kwargs["client"])

        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._core_app.StatusCommand.execute",
            capture_execute,
        )

        CoreApp().status(
            ctx=MagicMock(),
            organization_dir=tmp_path,
            selected=None,
            config_yaml=tmp_path / "config.dev.yaml",
            build_env_name=None,
            output_format=StatusOutputFormat.json,
            json_output=False,
            verbose=False,
        )

        assert clients == [None]


class TestEnsureEnabled:
    def test_noop_when_already_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(plugins.Plugin, "is_enabled", lambda self: True)

        def _fail_enable(name: str) -> bool:
            raise AssertionError("enable_plugin should not be called when the plugin is already enabled")

        monkeypatch.setattr(plugins.CDFToml, "enable_plugin", _fail_enable)

        # Should not raise.
        Plugins.data.ensure_enabled()

    def test_assume_yes_env_enables_and_continues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(plugins.Plugin, "is_enabled", lambda self: False)
        monkeypatch.setenv("CDF_ASSUME_YES", "1")
        enabled: list[str] = []

        def _enable(name: str) -> bool:
            enabled.append(name)
            return True

        monkeypatch.setattr(plugins.CDFToml, "enable_plugin", _enable)

        Plugins.data.ensure_enabled()

        assert enabled == ["data"]

    def test_non_interactive_aborts_with_hint(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(plugins.Plugin, "is_enabled", lambda self: False)
        monkeypatch.delenv("CDF_ASSUME_YES", raising=False)
        monkeypatch.setattr(plugins.sys.stdin, "isatty", lambda: False)
        monkeypatch.setattr(plugins.sys.stdout, "isatty", lambda: False)

        def _fail_enable(name: str) -> bool:
            raise AssertionError("enable_plugin should not be called when not confirmed")

        monkeypatch.setattr(plugins.CDFToml, "enable_plugin", _fail_enable)

        with pytest.raises(typer.Exit):
            Plugins.data.ensure_enabled()

        assert "is not enabled" in capsys.readouterr().out

    def test_interactive_confirm_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(plugins.Plugin, "is_enabled", lambda self: False)
        monkeypatch.delenv("CDF_ASSUME_YES", raising=False)
        monkeypatch.setattr(plugins.sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr(plugins.sys.stdout, "isatty", lambda: True)

        class _Confirm:
            def unsafe_ask(self) -> bool:
                return True

        monkeypatch.setattr(plugins.questionary, "confirm", lambda *args, **kwargs: _Confirm())
        enabled: list[str] = []

        def _enable(name: str) -> bool:
            enabled.append(name)
            return True

        monkeypatch.setattr(plugins.CDFToml, "enable_plugin", _enable)

        Plugins.data.ensure_enabled()

        assert enabled == ["data"]

    def test_interactive_decline_aborts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(plugins.Plugin, "is_enabled", lambda self: False)
        monkeypatch.delenv("CDF_ASSUME_YES", raising=False)
        monkeypatch.setattr(plugins.sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr(plugins.sys.stdout, "isatty", lambda: True)

        class _Confirm:
            def unsafe_ask(self) -> bool:
                return False

        monkeypatch.setattr(plugins.questionary, "confirm", lambda *args, **kwargs: _Confirm())

        def _fail_enable(name: str) -> bool:
            raise AssertionError("enable_plugin should not be called when declined")

        monkeypatch.setattr(plugins.CDFToml, "enable_plugin", _fail_enable)

        with pytest.raises(typer.Exit):
            Plugins.data.ensure_enabled()
