from unittest.mock import MagicMock

import pytest
import responses
from cognite.client import global_config
from cognite.client.credentials import OAuthClientCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auth import CLIENT_NAME
from cognite_toolkit._cdf_tk.utils.batch_processor import HTTPBatchProcessor


@pytest.fixture
def toolkit_config() -> ToolkitClientConfig:
    global_config.disable_pypi_version_check = True
    credentials = MagicMock(spec=OAuthClientCredentials)
    credentials.client_id = "toolkit-client-id"
    credentials.client_secret = "toolkit-client-secret"
    credentials.token_url = "https://toolkit.auth.com/oauth/token"
    credentials.scopes = ["ttps://pytest-field.cognitedata.com/.default"]
    credentials.authorization_header.return_value = "Auth", "Bearer dummy"
    return ToolkitClientConfig(
        client_name=CLIENT_NAME,
        project="pytest-project",
        credentials=credentials,
        is_strict_validation=False,
    )


class TestHTTPBatchProcessor:
    def test_happy_path(self, toolkit_config: ToolkitClientConfig) -> None:
        url = "http://example.com/api"
        processor = HTTPBatchProcessor[str](
            url,
            toolkit_config,
            lambda item: item["externalId"],
        )
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "item1"}, {"externalId": "item2"}]},
            )
            result = processor.process(({"externalId": "item1"}, {"externalId": "item2"}))

            assert len(rsps.calls) == 1
            assert result.total_successful == 2
