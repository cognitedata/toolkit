from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes.functions import Function
from cognite.client.data_classes.transformations import Transformation

from cognite_toolkit.cdf_tk.run import run_function, run_local_function, run_transformation
from cognite_toolkit.cdf_tk.utils import CDFToolConfig, get_oneshot_session
from tests.tests_unit.approval_client import ApprovalCogniteClient

THIS_FOLDER = Path(__file__).resolve().parent
DATA_FOLDER = THIS_FOLDER / "run_data"


def test_get_oneshot_session(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    session = get_oneshot_session(cdf_tool.client)
    assert session.id == 5192234284402249
    assert session.nonce == "QhlCnImCBwBNc72N"
    assert session.status == "READY"
    assert session.type == "ONESHOT_TOKEN_EXCHANGE"


def test_run_transformation(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    transformation = Transformation(
        name="Test transformation",
        external_id="test",
        query="SELECT * FROM timeseries",
    )
    cognite_client_approval.append(Transformation, transformation)

    assert run_transformation(cdf_tool, "test") is True


def test_run_function(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    function = Function(
        id=1234567890,
        name="Test function",
        external_id="test",
        description="Test function",
        owner="test",
        status="RUNNING",
        file_id=1234567890,
        function_path="./handler.py",
        created_time=int(datetime.now().timestamp() / 1000),
        secrets={"my_secret": "a_secret,"},
    )
    cognite_client_approval.append(Function, function)
    assert run_function(cdf_tool, external_id="test", payload='{"var1": "value"}', follow=False) is True
    cdf_tool.client.functions.calls.get_response.return_value = {}
    assert run_function(cdf_tool, external_id="test", payload='{"var1": "value"}', follow=True) is True


def test_run_local_function(cognite_client_approval: ApprovalCogniteClient) -> None:
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    function = Function(
        id=1234567890,
        name="Test function",
        external_id="fn_test2",
        description="Test function",
        owner="test",
        status="RUNNING",
        file_id=1234567890,
        function_path="./handler.py",
        created_time=int(datetime.now().timestamp() / 1000),
    )
    cognite_client_approval.append(Function, function)
    assert (
        run_local_function(
            ToolGlobals=cdf_tool,
            source_path=DATA_FOLDER,
            rebuild_env=True,
            build_env="dev",
            external_id="fn_test2",
            payload='{"var1": "value"}',
        )
        is True
    )
