import pytest
from cognite.client.credentials import OAuthClientCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig


class TestToolkitClientConfig:
    def test_create_app_url_raise_on_empty(self, toolkit_config: ToolkitClientConfig) -> None:
        with pytest.raises(ValueError, match="Endpoint must be a non-empty string"):
            toolkit_config.create_app_url("")

    @pytest.mark.parametrize(
        "token_url, expected_url",
        [
            pytest.param(
                "https://auth-dev.cognitedata-development.cognite.ai/oauth2/token",
                "https://auth-dev.cognitedata-development.cognite.ai/api/v1/principals/me",
                id="dev",
            ),
            pytest.param(
                "https://auth-staging.gc-bru-001.cogheim.net/oauth2/token",
                "https://auth-staging.gc-bru-001.cogheim.net/api/v1/principals/me",
                id="staging",
            ),
            pytest.param(
                "https://auth.cognite.com/oauth2/token",
                "https://auth.cognite.com/api/v1/principals/me",
                id="prod",
            ),
        ],
    )
    def test_create_auth_url(self, token_url: str, expected_url: str) -> None:
        config = ToolkitClientConfig(
            client_name="test-client",
            project="test-project",
            cluster="test-cluster",
            credentials=OAuthClientCredentials(
                token_url=token_url,
                client_id="my-client-id",
                client_secret="my-client-secret",
                scopes=None,
            ),
        )
        auth_url = config.create_auth_url("/principals/me")
        assert auth_url == expected_url
