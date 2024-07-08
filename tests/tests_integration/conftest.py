import os

import pytest
from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.client import ToolkitClient
from tests.constants import REPO_ROOT


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    load_dotenv(REPO_ROOT / ".env", override=True)
    cdf_cluster = os.environ["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=os.environ["IDP_TOKEN_URL"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        audience=f"https://{cdf_cluster}.cognitedata.com",
    )
    global_config.disable_pypi_version_check = True
    return CogniteClient(
        ClientConfig(
            client_name="cdf-toolkit-integration-tests",
            base_url=f"https://{cdf_cluster}.cognitedata.com",
            project=os.environ["CDF_PROJECT"],
            credentials=credentials,
        )
    )


@pytest.fixture(scope="session")
def toolkit_client(cognite_client: CogniteClient) -> ToolkitClient:
    return ToolkitClient(cognite_client._config)
