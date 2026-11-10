import builtins
import time
from typing import Any

from pydantic import JsonValue
from rich.json import JSON

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse, FunctionsActivation
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallResponse
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitMissingResourceError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content

MAX_POLL_INTERVAL_SECONDS = 10.0


class FunctionsAPICommand(ToolkitCommand):
    """Commands for calling the Cognite Functions API directly."""

    def retrieve(self, client: ToolkitClient, external_id: str) -> FunctionResponse:
        """Retrieve a function, including its deployment status.

        Args:
            client: Toolkit client used to call CDF.
            external_id: External ID of the function.

        Returns:
            The function as returned by CDF.
        """
        function = self._get_function(client, external_id)
        client.console.print(JSON.from_data(function.dump()))
        return function

    def call(
        self,
        client: ToolkitClient,
        external_id: str,
        json: str | None = None,
        wait: bool = True,
        timeout: int | None = None,
    ) -> FunctionCallResponse:
        """Call a function, by default wait for it to finish, and print the result.

        Args:
            client: Toolkit client used to call CDF.
            external_id: External ID of the function.
            json: Optional JSON/YAML object passed to the function as its ``data`` argument.
            wait: Whether to wait for the call to finish. If False, the created call is printed and returned.
            timeout: Maximum seconds to wait. Defaults to the function timeout of the CDF project.

        Returns:
            The function call. It is the finished call when waiting, otherwise the newly created call.

        Raises:
            ToolkitValueError: If a timeout is given together with wait=False, or the payload is invalid.
            ToolkitError: If waiting and the call does not complete successfully or does not finish in time.
        """
        if not wait and timeout is not None:
            raise ToolkitValueError("--timeout cannot be used together with --no-wait.")
        data = self._parse_payload(json)
        function = self._get_function(client, external_id)
        session = client.sessions.create_one_shot_token_exchange_session()
        call = client.functions.calls.call(function_id=function.id, nonce=session.nonce, data=data)
        if not wait:
            client.console.print(JSON.from_data(call.dump()))
            return call
        call = self._wait_for_completion(client, function.id, call, timeout)

        output: dict[str, JsonValue] = {"call": call.dump()}
        if call.status == "Completed":
            output["response"] = client.functions.calls.get_response(function.id, call.id).response
        output["logs"] = [log.dump() for log in client.functions.calls.get_logs(function.id, call.id)]
        client.console.print(JSON.from_data(output))

        if call.status != "Completed":
            message = f"Call {call.id} of function {external_id!r} ended with status {call.status!r}."
            raise ToolkitError(f"{message} {call.error}" if call.error else message)
        return call

    def _wait_for_completion(
        self, client: ToolkitClient, function_id: int, call: FunctionCallResponse, timeout: int | None
    ) -> FunctionCallResponse:
        max_seconds = timeout if timeout is not None else client.functions.limits().timeout_minutes * 60
        start = time.monotonic()
        sleep_seconds = 1.0
        while call.status.casefold() == "running":
            if time.monotonic() - start > max_seconds:
                raise ToolkitError(
                    f"Call {call.id} of the function did not finish within {max_seconds}s. "
                    "It may still be running in CDF."
                )
            time.sleep(sleep_seconds)
            sleep_seconds = min(sleep_seconds * 2, MAX_POLL_INTERVAL_SECONDS)
            retrieved = client.functions.calls.retrieve(function_id, [call.as_id()], ignore_unknown_ids=True)
            if not retrieved:
                raise ToolkitMissingResourceError(f"Could not find function call with id {call.id}.")
            call = retrieved[0]
        return call

    def list(self, client: ToolkitClient, limit: int = 25) -> builtins.list[FunctionResponse]:
        """List functions, including their deployment status.

        Args:
            client: Toolkit client used to call CDF.
            limit: Maximum number of functions to return.

        Returns:
            The functions as returned by CDF.
        """
        functions = client.functions.list(limit=limit)
        client.console.print(JSON.from_data([function.dump() for function in functions]))
        return functions

    def status(self, client: ToolkitClient) -> FunctionsActivation:
        """Get the Cognite Functions activation status for the project.

        Args:
            client: Toolkit client used to call CDF.

        Returns:
            Whether Functions are inactive, requested, or activated.
        """
        activation = client.functions.status()
        client.console.print(JSON.from_data(activation.dump()))
        return activation

    @staticmethod
    def _get_function(client: ToolkitClient, external_id: str) -> FunctionResponse:
        functions = client.functions.retrieve([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
        if not functions:
            raise ToolkitMissingResourceError(f"Function with external ID {external_id!r} does not exist.")
        return functions[0]

    @staticmethod
    def _parse_payload(json: str | None) -> dict[str, JsonValue] | None:
        if json is None:
            return None
        try:
            parsed: Any = read_yaml_content(json)
        except Exception as e:
            raise ToolkitValueError(f"Invalid JSON payload: {e}") from e
        if not isinstance(parsed, dict):
            raise ToolkitValueError("JSON payload must be an object.")
        return parsed
