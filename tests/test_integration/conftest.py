import os

import pytest
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from dotenv import load_dotenv

from tests.constants import REPO_ROOT


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    load_dotenv(REPO_ROOT / ".env")
    credentials = OAuthClientCredentials(
        token_url=os.environ["IDP_TOKEN_URL"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=[f"https://{os.environ['CDF_CLUSTER']}.cognitedata.com/.default"],
    )
    return CogniteClient(
        ClientConfig(
            client_name="cdf-toolkit-integration-tests",
            project=os.environ["CDF_PROJECT"],
            credentials=credentials,
        )
    )
