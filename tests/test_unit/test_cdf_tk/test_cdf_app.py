import pytest
import typer
import typer.main

from cognite_toolkit import _cdf
from cognite_toolkit._cdf_tk import plugins
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
