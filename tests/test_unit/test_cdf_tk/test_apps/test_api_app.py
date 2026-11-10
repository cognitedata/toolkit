from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps._api_app import APIApp


@pytest.fixture
def command() -> MagicMock:
    command = MagicMock()
    command.run.side_effect = lambda callback: callback()
    return command


class TestFunctionsApp:
    def test_retrieve_and_list_and_status_are_registered(self, command: MagicMock) -> None:
        with (
            patch("cognite_toolkit._cdf_tk.apps._api_app.FunctionsAPICommand", return_value=command),
            patch("cognite_toolkit._cdf_tk.apps._api_app.EnvironmentVariables"),
        ):
            runner = CliRunner()
            results = [
                runner.invoke(APIApp(), ["functions", "retrieve", "-e", "f"]),
                runner.invoke(APIApp(), ["functions", "list", "--limit", "3"]),
                runner.invoke(APIApp(), ["functions", "status"]),
            ]

        assert [r.exit_code for r in results] == [0, 0, 0], [r.output for r in results]
        command.retrieve.assert_called_once()
        command.list.assert_called_once()
        command.status.assert_called_once()

    def test_list_forwards_filters(self, command: MagicMock) -> None:
        with (
            patch("cognite_toolkit._cdf_tk.apps._api_app.FunctionsAPICommand", return_value=command),
            patch("cognite_toolkit._cdf_tk.apps._api_app.EnvironmentVariables") as env_vars,
        ):
            result = CliRunner().invoke(APIApp(), ["functions", "list", "-s", "failed", "-o", "Jane"])

        assert result.exit_code == 0, result.output
        command.list.assert_called_once_with(
            env_vars.create_from_environment.return_value.get_client.return_value,
            limit=25,
            status="Failed",
            owner="Jane",
        )
