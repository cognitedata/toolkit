from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps import MigrateApp
from cognite_toolkit._cdf_tk.apps._migrate_app import _validate_cdf_project
from cognite_toolkit._cdf_tk.exceptions import ToolkitValidationError


class TestValidateCdfProject:
    def test_matching_cli_argument_is_accepted(self) -> None:
        _validate_cdf_project("my-project", "my-project")

    def test_mismatched_cli_argument_raises(self) -> None:
        with pytest.raises(
            ToolkitValidationError,
            match=r"The CDF project in your command argument does not match your credentials, 'other-project'≠'my-project'\.",
        ):
            _validate_cdf_project("other-project", "my-project")

    def test_prompt_matching_project_is_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._migrate_app.questionary.text",
            lambda *args, **kwargs: MagicMock(unsafe_ask=lambda: "my-project"),
        )
        _validate_cdf_project(None, "my-project")

    def test_prompt_mismatched_project_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._migrate_app.questionary.text",
            lambda *args, **kwargs: MagicMock(unsafe_ask=lambda: "wrong"),
        )
        with pytest.raises(
            ToolkitValidationError,
            match=r"The CDF project you typed does not match your credentials, 'wrong'≠'my-project'\.",
        ):
            _validate_cdf_project(None, "my-project")


def _migrate_command_names() -> list[str]:
    return [command.name for command in MigrateApp().registered_commands if command.name]


class TestMigrateAppCdfProjectOption:
    @pytest.mark.parametrize("command_name", _migrate_command_names())
    def test_command_has_cdf_project_option(self, command_name: str) -> None:
        result = CliRunner().invoke(MigrateApp(), [command_name, "--help"])
        assert "--cdf-project" in result.output
