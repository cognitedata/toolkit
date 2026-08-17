import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps import AuthApp, CoreApp, DumpApp, ModulesApp


class TestGroupHelp:
    @pytest.mark.parametrize(
        "app_cls, expected_fragment",
        [
            (DumpApp, "datamodel"),
            (AuthApp, "init"),
            (ModulesApp, "init"),
        ],
    )
    def test_group_without_subcommand_prints_help(self, app_cls: type, expected_fragment: str) -> None:
        result = CliRunner().invoke(app_cls(), [])

        assert result.exit_code == 0
        assert expected_fragment in result.output
        assert "for more information" not in result.output

    def test_root_without_subcommand_still_shows_getting_started(self) -> None:
        result = CliRunner().invoke(CoreApp(), [])

        assert result.exit_code == 0
        assert "Getting started" in result.output
