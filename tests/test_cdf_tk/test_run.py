from unittest.mock import MagicMock

from requests import Response

from cognite_toolkit.cdf_tk.run import get_oneshot_session
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.conftest import ApprovalCogniteClient


def test_get_oneshot_session(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    cdf_tool.oauth_credentials = cognite_client_approval.mock_client
    cdf_tool.oauth_credentials.authorization_header.return_value = ("Bearer", "123")
    sessionResponse = Response()
    sessionResponse.status_code = 200
    sessionResponse._content = b'{"items":[{"id":5192234284402249,"nonce":"QhlCnImCBwBNc72N","status":"READY","type":"ONESHOT_TOKEN_EXCHANGE"}]}'
    cdf_tool.client.post.return_value = sessionResponse
    session = get_oneshot_session(cdf_tool)
    assert session.id == 5192234284402249
    assert session.nonce == "QhlCnImCBwBNc72N"
    assert session.status == "READY"
    assert session.type == "ONESHOT_TOKEN_EXCHANGE"


def test_run_transformation(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

    # cognite_client_approval.append(DataSet, first)
