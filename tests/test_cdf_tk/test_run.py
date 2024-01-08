from unittest.mock import MagicMock

from cognite.client.data_classes.capabilities import (
    SessionsAcl,
)

from cognite_toolkit.cdf_tk.run import get_oneshot_session
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.conftest import ApprovalCogniteClient
from tests.test_cdf_tk.conftest import get_capabilities_mock, get_post_mock


def test_get_oneshot_session(MockCDFToolConfig):
    MockCDFToolConfig._client.iam.token.inspect = get_capabilities_mock(
        [
            SessionsAcl(
                [SessionsAcl.Action.Create, SessionsAcl.Action.Delete, SessionsAcl.Action.List],
                scope=SessionsAcl.Scope.All(),
            ),
        ]
    )
    MockCDFToolConfig._client.post = get_post_mock(
        {
            "items": [
                {
                    "oneshotTokenExchange": True,
                },
            ],
        },
        b'{"items":[{"id":5192234284402249,"nonce":"QhlCnImCBwBNc72N","status":"READY","type":"ONESHOT_TOKEN_EXCHANGE"}]}',
    )
    session = get_oneshot_session(MockCDFToolConfig)
    assert session.id == 5192234284402249
    assert session.nonce == "QhlCnImCBwBNc72N"
    assert session.status == "READY"
    assert session.type == "ONESHOT_TOKEN_EXCHANGE"


def test_run_transformation(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

    # cognite_client_approval.append(DataSet, first)
