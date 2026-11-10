from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse, FunctionsActivation
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import (
    FunctionCallLogEntry,
    FunctionCallResponse,
    FunctionCallResult,
)
from cognite_toolkit._cdf_tk.commands.functions_api import FunctionsAPICommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitMissingResourceError, ToolkitValueError


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
    def _client_with_function() -> MagicMock:
        client = MagicMock()
        client.functions.retrieve.return_value = [
            FunctionResponse(id=1, external_id="my_function", name="My function", file_id=2, created_time=1)
        ]
        client.sessions.create_one_shot_token_exchange_session.return_value.nonce = "nonce"
        client.functions.limits.return_value.timeout_minutes = 10
        return client

    def test_call_waits_for_completion_and_returns_response(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        client = self._client_with_function()
        running = FunctionCallResponse(id=5, status="Running", start_time=1, function_id=1)
        completed = FunctionCallResponse(id=5, status="Completed", start_time=1, end_time=2, function_id=1)
        client.functions.calls.call.return_value = running
        client.functions.calls.retrieve.side_effect = [[running], [completed]]
        client.functions.calls.get_response.return_value = FunctionCallResult(
            function_id=1, call_id=5, response={"ok": True}
        )
        client.functions.calls.get_logs.return_value = [FunctionCallLogEntry(timestamp=1, message="hello")]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.call(client, external_id="my_function", json='{"key": "value"}')

        assert result == completed
        client.functions.calls.call.assert_called_once_with(function_id=1, nonce="nonce", data={"key": "value"})
        assert client.functions.calls.retrieve.call_count == 2
        client.functions.calls.get_response.assert_called_once_with(1, 5)

    def test_call_raises_when_call_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        client = self._client_with_function()
        failed = FunctionCallResponse(id=5, status="Failed", start_time=1, function_id=1, error="boom")
        client.functions.calls.call.return_value = failed
        client.functions.calls.get_logs.return_value = []
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitError, match="boom"):
            cmd.call(client, external_id="my_function")

        client.functions.calls.get_response.assert_not_called()

    def test_call_no_wait_returns_created_call(self) -> None:
        client = self._client_with_function()
        running = FunctionCallResponse(id=5, status="Running", start_time=1, function_id=1)
        client.functions.calls.call.return_value = running
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.call(client, external_id="my_function", wait=False)

        assert result == running
        client.functions.calls.retrieve.assert_not_called()
        client.functions.calls.get_response.assert_not_called()

    def test_call_no_wait_with_timeout_is_rejected(self) -> None:
        client = self._client_with_function()
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitValueError, match="cannot be used together"):
            cmd.call(client, external_id="my_function", wait=False, timeout=10)

        client.functions.calls.call.assert_not_called()

    def test_call_times_out(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("time.sleep", lambda _: None)
        client = self._client_with_function()
        running = FunctionCallResponse(id=5, status="Running", start_time=1, function_id=1)
        client.functions.calls.call.return_value = running
        client.functions.calls.retrieve.return_value = [running]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitError, match="did not finish within 1s"):
            cmd.call(client, external_id="my_function", timeout=1)

        client.functions.limits.assert_not_called()

    @pytest.mark.parametrize("payload", ["[1, 2]", "not: [valid"])
    def test_call_invalid_payload(self, payload: str) -> None:
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitValueError):
            cmd.call(MagicMock(), external_id="my_function", json=payload)

    def test_list_calls_api(self) -> None:
        client = MagicMock()
        function = FunctionResponse(id=1, external_id="my_function", name="My function", file_id=2, created_time=1)
        client.functions.list.return_value = [function]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.list(client, limit=10)

        assert result == [function]
        client.functions.list.assert_called_once_with(limit=10)

    def test_status_calls_api(self) -> None:
        client = MagicMock()
        activation = FunctionsActivation(status="activated")
        client.functions.status.return_value = activation
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        assert cmd.status(client) == activation
