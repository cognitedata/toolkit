import pytest

from cognite_toolkit._api import CogniteToolkit
from tests.tests_unit.approval_client import ApprovalCogniteClient


@pytest.fixture
def cognite_toolkit(cognite_client_approval: ApprovalCogniteClient) -> CogniteToolkit:
    toolkit = CogniteToolkit()
    toolkit.client = cognite_client_approval.client
    return toolkit
