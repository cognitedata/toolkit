import os
from typing import Any
from unittest import mock

import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingValueError
from cognite_toolkit._cdf_tk.utils.auth import (
    LOGIN_FLOW_DESCRIPTION,
    EnvironmentVariables,
    prompt_user_environment_variables,
)
from tests.test_unit.utils import MockQuestionary

PROJECT_AND_CLUSTER = {
    "CDF_CLUSTER": "toolkit-cluster",
    "CDF_PROJECT": "the-toolkit-project",
}

AUTH_MODULE = "cognite_toolkit._cdf_tk.utils.auth"


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
            pytest.param(
                {
                    **PROJECT_AND_CLUSTER,
                    "LOGIN_FLOW": "client_credentials",
                    "PROVIDER": "cdf",
                    "IDP_CLIENT_ID": "my-identifier",
                    "IDP_CLIENT_SECRET": "my-secret",
                },
                id="client-credentials cdf",
            ),
        ],
    )
    def test_get_valid_config(self, args: dict[str, Any]) -> None:
        env_vars = EnvironmentVariables(**args)

        missing_env_vars = env_vars.get_missing_vars()
        assert not missing_env_vars, f"Missing environment variables: {missing_env_vars}"

    def test_get_missing_vars(self) -> None:
        args = {
            **PROJECT_AND_CLUSTER,
            "LOGIN_FLOW": "client_credentials",
            "PROVIDER": "entra_id",
            "IDP_CLIENT_ID": "my-identifier",
        }

        with pytest.raises(ToolkitMissingValueError) as error:
            _ = EnvironmentVariables(**args).get_config(is_strict_validation=True)

        assert (
            str(error.value) == "The login flow 'client_credentials' requires the following environment variables: "
            "IDP_CLIENT_SECRET and IDP_TENANT_ID."
        )

    @mock.patch.dict(
        os.environ,
        {
            **PROJECT_AND_CLUSTER,
            "LOGIN_FLOW": "client_credentials",
            "IDP_CLIENT_ID": "my-identifier",
            "IDP_CLIENT_SECRET": "my-secret",
            "IDP_TENANT_ID": "my_tenant.onmicrosoft.com",
            "CDF_CLIENT_TIMEOUT": "10",
            "CDF_CLIENT_MAX_WORKERS": "5000",
        },
    )
    def test_create_environment_variables(
        self,
    ) -> None:
        env_vars = EnvironmentVariables.create_from_environment()

        assert env_vars.LOGIN_FLOW == "client_credentials"
        assert env_vars.CDF_CLUSTER == "toolkit-cluster"
        assert env_vars.CDF_PROJECT == "the-toolkit-project"
        assert env_vars.IDP_CLIENT_ID == "my-identifier"
        assert env_vars.IDP_CLIENT_SECRET == "my-secret"
        assert env_vars.IDP_TENANT_ID == "my_tenant.onmicrosoft.com"
        assert env_vars.CDF_CLIENT_TIMEOUT == 10
        assert env_vars.CDF_CLIENT_MAX_WORKERS == 5000

    def test_create_dot_env_file(self) -> None:
        env_vars = EnvironmentVariables(
            "bluefield",
            "cognite-toolkit",
            "entra_id",
            "client_credentials",
            IDP_CLIENT_ID="my-identifier",
            IDP_CLIENT_SECRET="my-secret",
            IDP_TENANT_ID="my_tenant.onmicrosoft.com",
        )

        assert (
            env_vars.create_dotenv_file()
            == """# .env file generated by cognite-toolkit
CDF_CLUSTER=bluefield
CDF_PROJECT=cognite-toolkit
PROVIDER=entra_id
LOGIN_FLOW=client_credentials

# Required variables
IDP_CLIENT_ID=my-identifier
IDP_CLIENT_SECRET=my-secret
IDP_TENANT_ID=my_tenant.onmicrosoft.com

# Optional variables (derived from the required variables)
CDF_URL=https://bluefield.cognitedata.com
IDP_TOKEN_URL=https://login.microsoftonline.com/my_tenant.onmicrosoft.com/oauth2/v2.0/token
IDP_AUDIENCE=https://bluefield.cognitedata.com
IDP_SCOPES=https://bluefield.cognitedata.com/.default
CDF_CLIENT_TIMEOUT=30
CDF_CLIENT_MAX_WORKERS=5
"""
        )


class TestPromptUserEnvironmentVariables:
    def test_device_code_is_first_login_flow(self) -> None:
        first_flow = next(iter(LOGIN_FLOW_DESCRIPTION))
        assert first_flow == "device_code", (
            "device_code should be the first login flow so new users see it as the default"
        )

    def test_new_user_defaults_to_device_code_entra_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Simulate a first-time user pressing Enter on every prompt:
        # provider=entra_id, flow=device_code, cluster=westeurope-1, project=my-project,
        # IDP_TENANT_ID=my-tenant, optional vars unchanged
        with MockQuestionary(
            AUTH_MODULE,
            monkeypatch,
            answers=[
                "entra_id",  # provider select
                "device_code",  # login flow select — this is the new default
                "westeurope-1",  # CDF cluster
                "my-project",  # CDF project
                "my-tenant.onmicrosoft.com",  # IDP_TENANT_ID
                False,  # "change optional vars?"
            ],
        ):
            env = prompt_user_environment_variables()

        assert env.LOGIN_FLOW == "device_code"
        assert env.PROVIDER == "entra_id"
        assert env.CDF_CLUSTER == "westeurope-1"
        assert env.CDF_PROJECT == "my-project"
        assert env.IDP_TENANT_ID == "my-tenant.onmicrosoft.com"
        assert not env.get_missing_vars()
