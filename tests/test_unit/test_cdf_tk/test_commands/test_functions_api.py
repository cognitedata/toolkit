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
    def _client_with_function(status: FunctionStatus | None = None) -> MagicMock:
        client = MagicMock()
        client.functions.retrieve.return_value = [
            FunctionResponse(
                id=1, external_id="my_function", name="My function", file_id=2, created_time=1, status=status
            )
        ]
        client.sessions.create_one_shot_token_exchange_session.return_value.nonce = "nonce"
        client.functions.limits.return_value.timeout_minutes = 10
        return client

    @staticmethod
    def _printed(client: MagicMock) -> Any:
        """The JSON that was printed to the console by the last print call."""
        return json.loads(client.console.print.call_args.args[0].text.plain)

    @pytest.fixture
    def fake_clock(self, monkeypatch: pytest.MonkeyPatch) -> list[float]:
        """Replaces time.sleep and time.monotonic with a clock that only advances when sleeping."""
        now = [0.0]
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        monkeypatch.setattr("time.sleep", sleep)
        monkeypatch.setattr("time.monotonic", lambda: now[0])
        return sleeps

    def test_call_waits_for_completion_and_prints_result(self, fake_clock: list[float]) -> None:
        client = self._client_with_function(status="Ready")
        running = FunctionCallResponse(id=5, status="Running", start_time=1, function_id=1)
        completed = FunctionCallResponse(id=5, status="Completed", start_time=1, end_time=2, function_id=1)
        client.functions.calls.call.return_value = running
        client.functions.calls.retrieve.side_effect = [[running], [completed]]
        client.functions.calls.get_response.return_value = FunctionCallResult(
            function_id=1, call_id=5, response={"ok": True}
        )
        client.functions.calls.get_logs.return_value = [FunctionCallLogEntry(timestamp=1, message="hello")]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.call(client, external_id="my_function", payload='{"key": "value"}')

        assert result == completed
        client.functions.calls.call.assert_called_once_with(function_id=1, nonce="nonce", data={"key": "value"})
        assert fake_clock == [1.0, 2.0]
        output = self._printed(client)
        assert output["call"]["status"] == "Completed"
        assert output["response"] == {"ok": True}
        assert output["logs"] == [{"timestamp": 1, "message": "hello"}]

    @pytest.mark.parametrize("status", ["Failed", "Timeout", "ConcurrencyLimitExceeded"])
    def test_call_raises_and_prints_logs_when_call_does_not_complete(self, status: str) -> None:
        client = self._client_with_function()
        failed = FunctionCallResponse(id=5, status=status, start_time=1, function_id=1, error="boom")
        client.functions.calls.call.return_value = failed
        client.functions.calls.get_logs.return_value = [FunctionCallLogEntry(timestamp=1, message="trace")]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitError, match=f"{status}.*boom"):
            cmd.call(client, external_id="my_function")

        client.functions.calls.get_response.assert_not_called()
        output = self._printed(client)
        assert "response" not in output
        assert output["logs"] == [{"timestamp": 1, "message": "trace"}]

    def test_call_no_wait_returns_created_call(self) -> None:
        client = self._client_with_function()
        running = FunctionCallResponse(id=5, status="Running", start_time=1, function_id=1)
        client.functions.calls.call.return_value = running
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        result = cmd.call(client, external_id="my_function", wait=False)

        assert result == running
        assert self._printed(client)["id"] == 5
        client.functions.calls.retrieve.assert_not_called()
        client.functions.calls.get_response.assert_not_called()
        client.functions.calls.get_logs.assert_not_called()

    def test_call_no_wait_with_timeout_is_rejected(self) -> None:
        client = self._client_with_function()
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitValueError, match="cannot be used together"):
            cmd.call(client, external_id="my_function", wait=False, timeout=10)

        client.functions.retrieve.assert_not_called()
        client.functions.calls.call.assert_not_called()

    def test_call_times_out_without_overshooting_and_prints_logs(self, fake_clock: list[float]) -> None:
        client = self._client_with_function()
        running = FunctionCallResponse(id=5, status="Running", start_time=1, function_id=1)
        client.functions.calls.call.return_value = running
        client.functions.calls.retrieve.return_value = [running]
        client.functions.calls.get_logs.return_value = [FunctionCallLogEntry(timestamp=1, message="still going")]
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitError, match=r"Call 5 .* did not finish within 5s.*may still be running"):
            cmd.call(client, external_id="my_function", timeout=5)

        assert fake_clock == [1.0, 2.0, 2.0]  # The last sleep is cut short at the deadline.
        client.functions.limits.assert_not_called()
        client.functions.calls.get_response.assert_not_called()
        assert self._printed(client)["logs"] == [{"timestamp": 1, "message": "still going"}]

    @pytest.mark.parametrize("status", ["Queued", "Deploying", "Failed", "Retired"])
    def test_call_rejects_function_that_is_not_ready(self, status: FunctionStatus) -> None:
        client = self._client_with_function(status=status)
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitError, match=f"status '{status}', not 'Ready'"):
            cmd.call(client, external_id="my_function")

        client.sessions.create_one_shot_token_exchange_session.assert_not_called()
        client.functions.calls.call.assert_not_called()

    @pytest.mark.parametrize("payload", ["[1, 2]", "not: [valid"])
    def test_call_invalid_payload(self, payload: str) -> None:
        cmd = FunctionsAPICommand(skip_tracking=True, silent=True)

        with pytest.raises(ToolkitValueError):
            cmd.call(MagicMock(), external_id="my_function", payload=payload)

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
