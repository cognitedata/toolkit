from unittest.mock import MagicMock

import pytest
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ClientCredentials, OidcCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitRequiredValueError, ToolkitTypeError
from cognite_toolkit._cdf_tk.utils.cdf import read_auth, try_find_error


class TestTryFindError:
    @pytest.mark.parametrize(
        "credentials, expected",
        [
            pytest.param(
                ClientCredentials("${ENVIRONMENT_VAR}", "1234"),
                "The environment variable is not set: ENVIRONMENT_VAR.",
                id="Missing environment variable",
            ),
            pytest.param(
                ClientCredentials("${ENV1}", "${ENV2}"),
                "The environment variables are not set: ENV1 and ENV2.",
                id="Missing environment variable",
            ),
            pytest.param(
                OidcCredentials(
                    client_id="my-client-id",
                    client_secret="123",
                    scopes=["https://cognite.com"],
                    token_uri="not-valid-uri",
                    cdf_project_name="my-project",
                ),
                "The tokenUri 'not-valid-uri' is not a valid URI.",
            ),
            pytest.param(None, None, id="empty"),
        ],
    )
    def test_try_find_error(self, credentials: OidcCredentials | ClientCredentials | None, expected: str | None):
        assert try_find_error(credentials) == expected


class TestReadAuth:
    @pytest.mark.parametrize(
        "auth, expected",
        [
            pytest.param(
                {
                    "clientId": "my_id",
                    "clientSecret": "my_secret",
                },
                ClientCredentials("my_id", "my_secret"),
                id="Client credentials",
            ),
            pytest.param(
                {
                    "clientId": "my_id",
                    "clientSecret": "my_secret",
                    "tokenUri": "https://my-token-uri",
                    "cdfProjectName": "my-project",
                },
                OidcCredentials("my_id", "my_secret", "https://my-token-uri", "my-project"),
                id="OIDC credentials only required",
            ),
            pytest.param(
                {
                    "clientId": "my_id",
                    "clientSecret": "my_secret",
                    "tokenUri": "https://my-token-uri",
                    "cdfProjectName": "my-project",
                    "scopes": "USER_IMPERSONATION,https://cognite.com",
                    "audience": "https://cognite.com",
                },
                OidcCredentials(
                    "my_id",
                    "my_secret",
                    "https://my-token-uri",
                    "my-project",
                    ["USER_IMPERSONATION", "https://cognite.com"],
                    "https://cognite.com",
                ),
                id="OIDC credentials all fields",
            ),
        ],
    )
    def test_read_valid_auth(self, auth: object, expected: ClientCredentials | OidcCredentials) -> None:
        config = MagicMock(spec=ToolkitClientConfig)
        result = read_auth(auth, config, "only-used-in-errors", "only-used-in-errors", allow_oidc=True)
        assert isinstance(result, (ClientCredentials, OidcCredentials))
        assert result.dump() == expected.dump()

    @pytest.mark.parametrize(
        "auth, expected_exception",
        [
            pytest.param(
                None,
                ToolkitRequiredValueError("Authentication is missing for compute resource 'my_compute_resource'."),
                id="Missing authentication",
            ),
            pytest.param(
                123,
                ToolkitTypeError("Authentication must be a dictionary for compute resource 'my_compute_resource'"),
            ),
            pytest.param(
                {"clientId": "my_id"},
                ToolkitRequiredValueError(
                    "Authentication must contain clientId and clientSecret for compute resource 'my_compute_resource'"
                ),
            ),
        ],
    )
    def test_read_invalid_auth(self, auth: object, expected_exception: ToolkitError) -> None:
        with pytest.raises(type(expected_exception)) as excinfo:
            config = MagicMock(spec=ToolkitClientConfig)
            config.is_strict_validation = True
            read_auth(auth, config, "my_compute_resource", "compute resource")

        assert str(excinfo.value) == str(expected_exception)

    def test_read_warning_auth(self) -> None:
        credentials = OAuthClientCredentials("url", "my_id", "my_secret", ["USER_IMPERSONATION"])
        config = MagicMock(spec=ToolkitClientConfig)
        config.is_strict_validation = False
        config.credentials = credentials
        warning: str = ""

        def catch_warning_message(*messages: object) -> None:
            nonlocal warning
            warning = "".join(map(str, messages))

        console = MagicMock()
        console.print = catch_warning_message

        result = read_auth(None, config, "my_compute_resource", "compute resource", console=console)
        assert (
            "Authentication is missing for compute resource 'my_compute_resource'. "
            "Falling back to the Toolkit credentials"
        ) in warning
        assert isinstance(result, ClientCredentials)
        assert result.dump() == {
            "clientId": "my_id",
            "clientSecret": "my_secret",
        }
