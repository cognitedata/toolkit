from unittest.mock import MagicMock, patch

import pytest
import responses
from cognite.client import global_config
from cognite.client.data_classes import FunctionWrite
from cognite.client.exceptions import CogniteAPIError
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.extended_functions import ExtendedFunctionsAPI


class TestExtendedFunctionsAPI:
    def test_create_function_200(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/functions"
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "test_function", "name": "test_function", "createdTime": 42}]},
            )
            result = client.functions.create_with_429_retry(fun)

        assert result.external_id == "test_function"
        assert result.name == "test_function"
        assert result.created_time == 42

    def test_create_function_429_succeed(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/functions"
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")
        console = MagicMock(spec=Console)
        with responses.RequestsMock() as rsps, patch(f"{ExtendedFunctionsAPI.__module__}.time.sleep"):
            for i in range(global_config.max_retries - 1):
                rsps.add(
                    responses.POST, url, status=429, json={"error": "Too many requests"}, headers={"Retry-After": "42"}
                )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "test_function", "name": "test_function", "createdTime": 42}]},
            )

            result = client.functions.create_with_429_retry(fun, console=console)
        assert result.external_id == "test_function"
        assert console.print.call_count == global_config.max_retries - 1
        assert console.print.call_args.args[1] == "Rate limit exceeded. Retrying after 42 seconds."

    def test_create_function_429_exceed_max_retries(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/functions"
        fun = FunctionWrite(name="test_function", file_id=123, external_id="test_function")
        console = MagicMock(spec=Console)
        with responses.RequestsMock() as rsps, patch(f"{ExtendedFunctionsAPI.__module__}.time.sleep"):
            for i in range(global_config.max_retries + 1):
                rsps.add(
                    responses.POST, url, status=429, json={"error": "Too many requests"}, headers={"Retry-After": "42"}
                )
            with pytest.raises(CogniteAPIError) as exc_info:
                client.functions.create_with_429_retry(fun, console=console)
        assert console.print.call_count == global_config.max_retries
        assert exc_info.value.message == "Too many requests"
        assert exc_info.value.code == 429
