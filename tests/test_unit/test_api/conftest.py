import os

import pytest

from cognite_toolkit._api import CogniteToolkit
from tests.test_unit.approval_client import ApprovalToolkitClient


@pytest.fixture
def set_environment_variables() -> None:
    environment_variables = {
        "CDF_PROJECT": "pytest-project",
        "CDF_CLUSTER": "bluefield",
        "IDP_TOKEN_URL": "dummy",
        "IDP_CLIENT_ID": "dummy",
        "IDP_CLIENT_SECRET": "dummy",
        "IDP_TENANT_ID": "dummy",
        "IDP_AUDIENCE": "https://bluefield.cognitedata.com",
        "IDP_SCOPES": "https://bluefield.cognitedata.com/.default",
        "CDF_URL": "https://bluefield.cognitedata.com",
    }
    existing = {}
    for key, value in environment_variables.items():
        existing[key] = os.environ.get(key)
        os.environ[key] = value

    yield None

    for key, value in existing.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value


@pytest.fixture
def cognite_toolkit(toolkit_client_approval: ApprovalToolkitClient, set_environment_variables: None) -> CogniteToolkit:
    toolkit = CogniteToolkit()
    toolkit.client = toolkit_client_approval.client
    return toolkit
