import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import (
    FunctionResponse,
    FunctionsActivation,
    FunctionStatus,
)
from cognite_toolkit._cdf_tk.commands.functions_api import FunctionsAPICommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError


class TestFunctionsAPICommand:
    def test_retrieve_calls_api(self) -> None:
        client = MagicMock()
        function = FunctionResponse(
            id=1, external_id="my_function", name="My function", file_id=2, created_time=1, status="Ready"
        )
        client.functions.retrieve.return_value = [function]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.retrieve(client, external_id="my_function")

        assert result == function
        client.functions.retrieve.assert_called_once_with(
            [ExternalId(external_id="my_function")], ignore_unknown_ids=True
        )

    def test_retrieve_missing_function(self) -> None:
        client = MagicMock()
        client.functions.retrieve.return_value = []
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitMissingResourceError, match="does not exist"):
            cmd.retrieve(client, external_id="missing")

    @staticmethod
    def _printed(client: MagicMock) -> Any:
        """The JSON that was printed to the console by the last print call."""
        return json.loads(client.console.print.call_args.args[0].text.plain)

    def test_list_calls_api(self) -> None:
        client = MagicMock()
        function = FunctionResponse(id=1, external_id="my_function", name="My function", file_id=2, created_time=1)
        client.functions.list.return_value = [function]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.list(client, limit=10)

        assert result == [function]
        client.functions.list.assert_called_once_with(limit=10)
        assert [item["externalId"] for item in self._printed(client)] == ["my_function"]

    def test_list_filters_by_status_and_owner_before_applying_limit(self) -> None:
        client = MagicMock()
        rows: list[tuple[FunctionStatus, str]] = [
            ("Failed", "a"),
            ("Ready", "a"),
            ("Ready", "b"),
            ("Ready", "a"),
            ("Ready", "a"),
        ]
        functions = [
            FunctionResponse(id=i, external_id=f"f{i}", name="F", file_id=2, created_time=1, status=status, owner=owner)
            for i, (status, owner) in enumerate(rows)
        ]
        client.functions.list.return_value = functions
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.list(client, limit=2, status="Ready", owner="a")

        assert [function.external_id for function in result] == ["f1", "f3"]
        client.functions.list.assert_called_once_with(limit=None)

    def test_status_calls_api(self) -> None:
        client = MagicMock()
        activation = FunctionsActivation(status="activated")
        client.functions.status.return_value = activation
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        assert cmd.status(client) == activation
        assert self._printed(client) == {"status": "activated"}
