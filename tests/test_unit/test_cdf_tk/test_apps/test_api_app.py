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
    def test_run_forwards_short_options(self, command: MagicMock) -> None:
        with (
            patch("cognite_toolkit._cdf_tk.apps._api_app.FunctionsAPICommand", return_value=command),
            patch("cognite_toolkit._cdf_tk.apps._api_app.EnvironmentVariables") as env_vars,
        ):
            result = CliRunner().invoke(
                APIApp(), ["functions", "run", "-e", "my_function", "-j", '{"a": 1}', "-t", "30"]
            )

        assert result.exit_code == 0, result.output
        command.call.assert_called_once_with(
            env_vars.create_from_environment.return_value.get_client.return_value,
            external_id="my_function",
            payload='{"a": 1}',
            wait=True,
            timeout=30,
        )

    def test_run_no_wait(self, command: MagicMock) -> None:
        with (
            patch("cognite_toolkit._cdf_tk.apps._api_app.FunctionsAPICommand", return_value=command),
            patch("cognite_toolkit._cdf_tk.apps._api_app.EnvironmentVariables"),
        ):
            result = CliRunner().invoke(APIApp(), ["functions", "run", "-e", "my_function", "--no-wait"])

        assert result.exit_code == 0, result.output
        assert command.call.call_args.kwargs["wait"] is False

    @pytest.mark.parametrize(
        ("arguments", "message"),
        [
            (["-e", "f", "--no-wait", "--timeout", "10"], "cannot be used together"),
            (["-e", "f", "--timeout", "0"], "0 is not in the range"),
            ([], "Missing option"),
        ],
    )
    def test_run_rejects_invalid_options_before_creating_client(self, arguments: list[str], message: str) -> None:
        with patch("cognite_toolkit._cdf_tk.apps._api_app.EnvironmentVariables") as env_vars:
            result = CliRunner().invoke(APIApp(), ["functions", "run", *arguments], env={"COLUMNS": "200"})

        assert result.exit_code == 2
        assert message in result.output
        env_vars.create_from_environment.assert_not_called()

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
