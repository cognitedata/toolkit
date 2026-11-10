from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse, FunctionsActivation
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

    def test_status_calls_api(self) -> None:
        client = MagicMock()
        activation = FunctionsActivation(status="activated")
        client.functions.status.return_value = activation
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        assert cmd.status(client) == activation
