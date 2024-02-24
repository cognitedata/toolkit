import pytest

from cognite_toolkit import CogniteToolkit
from tests.tests_unit.approval_client import ApprovalCogniteClient


@pytest.fixture
def cognite_toolkit(cognite_client_approval: ApprovalCogniteClient) -> CogniteToolkit:
    return CogniteToolkit(cognite_client_approval.client)
