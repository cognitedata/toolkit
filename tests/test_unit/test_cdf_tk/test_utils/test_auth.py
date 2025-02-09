from multiprocessing.context import AuthenticationError
from typing import Any

import pytest
from requests.exceptions import ConnectionError

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auth2 import EnvironmentVariables

PROJECT_AND_CLUSTER = {
    "CDF_CLUSTER": "toolkit-cluster",
    "CDF_PROJECT": "the-toolkit-project",
}


class TestEnvironmentVariables:
    @pytest.mark.parametrize(
        "args",
        [
            pytest.param({**PROJECT_AND_CLUSTER, "LOGIN_FLOW": "token", "CDF_TOKEN": "super-token"}, id="token flow"),
            pytest.param(
                {
                    **PROJECT_AND_CLUSTER,
                    "LOGIN_FLOW": "client_credentials",
                    "PROVIDER": "entra_id",
                    "IDP_TENANT_ID": "my_tenant.onmicrosoft.com",
                    "IDP_CLIENT_ID": "my-identifier",
                    "IDP_CLIENT_SECRET": "my***super***secret",
                },
                id="client-credentials entra",
            ),
            pytest.param(
                {
                    **PROJECT_AND_CLUSTER,
                    "LOGIN_FLOW": "client_credentials",
                    "PROVIDER": "other",
                    "IDP_TOKEN_URL": "https://auth.login.my_domian.io/oauth/token",
                    "IDP_AUDIENCE": "https://toolkit-cluster.fusion.cognite.com/the-toolkit-project",
                    "IDP_SCOPES": "IDENTITY,ADMIN,client:cognite-cicd@my_domain.io,user_impersonation",
                    "IDP_CLIENT_ID": "my-identifier",
                    "IDP_CLIENT_SECRET": "my***super***secret",
                },
                id="client-credentials other (auth0)",
            ),
            pytest.param(
                {
                    **PROJECT_AND_CLUSTER,
                    "LOGIN_FLOW": "interactive",
                    "PROVIDER": "entra_id",
                    "IDP_TENANT_ID": "my_tenant.onmicrosoft.com",
                    "IDP_CLIENT_ID": "my-identifier",
                },
                id="interactive entra",
            ),
            pytest.param(
                {
                    **PROJECT_AND_CLUSTER,
                    "LOGIN_FLOW": "device_code",
                    "PROVIDER": "entra_id",
                    "IDP_TENANT_ID": "my_tenant.onmicrosoft.com",
                },
                id="device enta",
            ),
            pytest.param(
                {
                    **PROJECT_AND_CLUSTER,
                    "LOGIN_FLOW": "device_code",
                    "PROVIDER": "other",
                    "IDP_CLIENT_ID": "my-identifier",
                    "IDP_DISCOVERY_URL": "https://auth.login.my_domian.io/oauth",
                },
                id="device other",
            ),
        ],
    )
    def test_get_valid_config(self, args: dict[str, Any]) -> None:
        env_vars = EnvironmentVariables(**args)

        try:
            config = env_vars.get_config()
        except (KeyError, AuthenticationError) as e:
            assert False, f"Failed to get config: {e}"
        except (ValueError, ConnectionError):
            # When we try to instantiate config for interactive login, we get an error
            # because the domain is not valid. In this test we are only interested in
            # the config object, so we ignore this error.
            assert True
        else:
            assert isinstance(config, ToolkitClientConfig)
