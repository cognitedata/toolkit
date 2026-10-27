import sys
from pathlib import Path

import pytest
import typer
from pytest import MonkeyPatch

from cognite_toolkit import _cdf
from tests.test_unit.utils import MockQuestionary

CDF_TOML_WITH_COMMENTS = """[cdf]
# The default environment
default_env = "dev"

[plugins]
# Plugins enable optional commands
#data = true # The data plugin
run = true # The run plugin
data = false # The data plugin

[modules]
# This is the version of the modules. It should not be changed manually.
# It will be updated by the 'cdf modules upgrade' command.
version = "0.0.0"
"""


@pytest.fixture()
def minimal_app(monkeypatch: MonkeyPatch) -> typer.Typer:
    """A minimal Typer app where no plugin commands are registered."""
    app = typer.Typer(pretty_exceptions_enable=False)

    @app.command("about")
    def about() -> None: ...

    @app.command("build")
    def build() -> None: ...

    monkeypatch.setattr(_cdf, "_app", app)
    return app


def _run_cli(monkeypatch: MonkeyPatch, *args: str) -> SystemExit:
    monkeypatch.setattr(sys, "argv", ["cdf", *args])
    with pytest.raises(SystemExit) as exc_info:
        _cdf.app()
    return exc_info.value


class TestCommandNotFound:
    def test_unknown_command_not_plugin(
        self, minimal_app: typer.Typer, monkeypatch: MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        monkeypatch.chdir(tmp_path)
        with MockQuestionary(_cdf.__name__, []):
            exit_err = _run_cli(monkeypatch, "notacommand")

        assert exit_err.code != 0
        assert "not enabled" not in capsys.readouterr().out

    def test_plugin_command_user_declines(
        self, minimal_app: typer.Typer, monkeypatch: MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        cdf_toml = tmp_path / "cdf.toml"
        cdf_toml.write_text(CDF_TOML_WITH_COMMENTS, encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        with MockQuestionary(_cdf.__name__, [False]) as mock:
            exit_err = _run_cli(monkeypatch, "data")
            remaining = list(mock.answers)

        assert exit_err.code != 0
        assert remaining == [], "The user should have been asked to enable the plugin"
        assert "The plugin data is not enabled" in capsys.readouterr().out
        assert cdf_toml.read_text(encoding="utf-8") == CDF_TOML_WITH_COMMENTS

    def test_plugin_command_user_accepts_updates_cdf_toml(
        self, minimal_app: typer.Typer, monkeypatch: MonkeyPatch, tmp_path: Path
    ) -> None:
        cdf_toml = tmp_path / "cdf.toml"
        cdf_toml.write_text(CDF_TOML_WITH_COMMENTS, encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        with MockQuestionary(_cdf.__name__, [True]) as mock:
            _run_cli(monkeypatch, "data")
            remaining = list(mock.answers)

        assert remaining == []
        assert cdf_toml.read_text(encoding="utf-8") == CDF_TOML_WITH_COMMENTS.replace(
            "data = false # The data plugin", "data = true # The data plugin"
        )

    def test_plugin_command_user_accepts_adds_missing_plugin_key(
        self, minimal_app: typer.Typer, monkeypatch: MonkeyPatch, tmp_path: Path
    ) -> None:
        cdf_toml = tmp_path / "cdf.toml"
        cdf_toml.write_text(CDF_TOML_WITH_COMMENTS, encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        with MockQuestionary(_cdf.__name__, [True]):
            _run_cli(monkeypatch, "dump")

        assert cdf_toml.read_text(encoding="utf-8") == CDF_TOML_WITH_COMMENTS.replace(
            "[plugins]\n", "[plugins]\ndump = true\n"
        )

    def test_plugin_command_user_accepts_adds_missing_plugin_key_with_comments(
        self, minimal_app: typer.Typer, monkeypatch: MonkeyPatch, tmp_path: Path
    ) -> None:
        content = CDF_TOML_WITH_COMMENTS.replace("data = false # The data plugin", "")
        cdf_toml = tmp_path / "cdf.toml"
        cdf_toml.write_text(content, encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        with MockQuestionary(_cdf.__name__, [True]):
            _run_cli(monkeypatch, "data")

        assert cdf_toml.read_text(encoding="utf-8") == content.replace(
            "[plugins]\n", "[plugins]\ndata = true # The data plugin\n"
        )

    def test_plugin_command_user_accepts_adds_missing_plugins_section(
        self, minimal_app: typer.Typer, monkeypatch: MonkeyPatch, tmp_path: Path
    ) -> None:
        content = CDF_TOML_WITH_COMMENTS.split("[plugins]")[0]
        cdf_toml = tmp_path / "cdf.toml"
        cdf_toml.write_text(content, encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        with MockQuestionary(_cdf.__name__, [True]):
            _run_cli(monkeypatch, "dev")

        assert cdf_toml.read_text(encoding="utf-8") == content + "\n[plugins]\ndev = true\n"
