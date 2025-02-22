import pytest
from cognite.client.data_classes import ClientCredentials, OidcCredentials

from cognite_toolkit._cdf_tk.utils.cdf import try_find_error


class TestTryFindError:
    @pytest.mark.parametrize(
        "credentials, expected",
        [
            pytest.param(
                ClientCredentials("${ENVIRONMENT_VAR}", "1234"),
                "The environment variable 'ENVIRONMENT_VAR' is not set.",
                id="Missing environment variable",
            ),
            pytest.param(
                OidcCredentials(
                    "my-client-id", "123", [["https://cognite.com"]], "https://cognite.com/token", "my-project"
                ),
                "The scopes is expected to be a list of strings, but got a list of lists.",
            ),
            pytest.param(None, None, id="empty"),
        ],
    )
    def test_try_find_error(self, credentials: OidcCredentials | ClientCredentials | None, expected: str | None):
        assert try_find_error(credentials) == expected
