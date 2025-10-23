import json
from unittest.mock import MagicMock, patch

import pytest
import respx
from cognite.client import global_config
from cognite.client.data_classes import FunctionWrite
from httpx import Response
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ToolkitAPIError


class TestExtendedFunctionsAPI:
    def test_create_function_200(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions")
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")

        respx_mock.post(url).mock(
            return_value=Response(
                status_code=200,
                json={"items": [{"externalId": "test_function", "name": "test_function", "createdTime": 42}]},
            )
        )
        result = client.functions.create_with_429_retry(fun)

        assert result.external_id == "test_function"
        assert result.name == "test_function"
        assert result.created_time == 42

    def test_create_function_429_succeed(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions")
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")
        console = MagicMock(spec=Console)

        with patch(f"{HTTPClient.__module__}.time"):
            # Add multiple 429 responses followed by a success response
            responses = [
                Response(status_code=429, json={"error": "Too many requests"}, headers={"Retry-After": "42"})
                for _ in range(global_config.max_retries - 1)
            ]
            responses.append(
                Response(
                    status_code=200,
                    json={"items": [{"externalId": "test_function", "name": "test_function", "createdTime": 42}]},
                )
            )
            respx_mock.post(url).mock(side_effect=responses)

            result = client.functions.create_with_429_retry(fun, console=console)
        assert result.external_id == "test_function"
        assert console.print.call_count == global_config.max_retries - 1
        assert console.print.call_args.args[1] == (
            "Rate limit exceeded for the '/functions' endpoint. Retrying after 42.0 seconds."
        )

    def test_create_function_429_exceed_max_retries(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions")
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")
        console = MagicMock(spec=Console)

        with patch(f"{HTTPClient.__module__}.time"):
            # Mock to always return 429 responses
            respx_mock.post(url).mock(
                return_value=Response(
                    status_code=429, json={"error": "Too many requests"}, headers={"Retry-After": "42"}
                )
            )

            with pytest.raises(ToolkitAPIError) as exc_info:
                client.functions.create_with_429_retry(fun, console=console)
        assert console.print.call_count == global_config.max_retries
        assert "Too many requests" in str(exc_info.value)
        assert (
            "Rate limit exceeded for the '/functions' endpoint. Retrying after 42.0 seconds."
        ) in console.print.call_args.args[1]

    def test_create_function_429_invalid_retry_after(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions")
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")
        console = MagicMock(spec=Console)

        with patch(f"{HTTPClient.__module__}.time"):
            respx_mock.post(url).mock(
                return_value=Response(
                    status_code=429, json={"error": "Too many requests"}, headers={"Retry-After": "invalid"}
                )
            )

            with pytest.raises(ToolkitAPIError) as exc_info:
                client.functions.create_with_429_retry(fun, console=console)
        assert console.print.call_count == 0
        assert "Too many requests" in str(exc_info.value)

    def test_create_function_401(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions")
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")

        from cognite_toolkit._cdf_tk.utils.http_client import ToolkitAPIError

        respx_mock.post(url).mock(return_value=Response(status_code=401, json={"error": "Unauthorized"}))

        with pytest.raises(ToolkitAPIError) as exc_info:
            client.functions.create_with_429_retry(fun)

        assert "401" in str(exc_info.value)
        assert "Unauthorized" in str(exc_info.value)
        # Verify that only one request was made
        assert len(respx_mock.calls) == 1
        assert respx_mock.calls[0].request.url == url
        assert respx_mock.calls[0].request.method == "POST"

    def test_create_function_invalid_json_float(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function", cpu=float("inf"))

        with pytest.raises(ToolkitAPIError) as exc_info:
            client.functions.create_with_429_retry(fun)

        assert "Out of range float values are not JSON compliant" in str(exc_info.value)

    def test_create_function_invalid_json(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        fun = FunctionWrite(
            name="test_function",
            file_id=123,
            external_id="test_function",
            cpu=43j,  # Complex number, which is not JSON serializable
        )
        with pytest.raises(ToolkitAPIError) as exc_info:
            client.functions.create_with_429_retry(fun)

        assert "Object 43j of type <class 'complex'> can't be serialized by the JSON encoder" in str(exc_info.value)

    @pytest.mark.usefixtures("disable_gzip")
    def test_delete_function_200(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions/delete")

        respx_mock.post(url).mock(return_value=Response(status_code=200, json={}))

        # Should not raise any exception
        client.functions.delete_with_429_retry(["test_function"], ignore_unknown_ids=True)

        assert len(respx_mock.calls) == 1
        call = respx_mock.calls[0]
        assert call.request.url == url
        assert json.loads(call.request.content) == {
            "items": [{"externalId": "test_function"}],
            "ignoreUnknownIds": True,
        }

    def test_delete_function_429_succeed(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config, enable_set_pending_ids=True)
        url = config.create_api_url("/functions/delete")
        console = MagicMock(spec=Console)

        with patch(f"{HTTPClient.__module__}.time"):
            # Add multiple 429 responses followed by a success response
            responses = [
                Response(status_code=429, json={"error": "Too many requests"}, headers={"Retry-After": "42"})
                for _ in range(global_config.max_retries - 1)
            ]
            responses.append(Response(status_code=200, json={}))
            respx_mock.post(url).mock(side_effect=responses)

            # Should not raise any exception
            client.functions.delete_with_429_retry(["test_function"], ignore_unknown_ids=True, console=console)
        assert console.print.call_count == global_config.max_retries - 1
        assert console.print.call_args.args[1] == (
            "Rate limit exceeded for the '/functions/delete' endpoint. Retrying after 42.0 seconds."
        )
