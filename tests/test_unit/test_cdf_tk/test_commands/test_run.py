from datetime import datetime
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.functions import Function, FunctionCall
from cognite.client.data_classes.transformations import Transformation

from cognite_toolkit._cdf_tk.commands import RunFunctionCommand, RunTransformationCommand
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, get_oneshot_session
from tests.data import RUN_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


def test_get_oneshot_session(toolkit_client_approval: ApprovalToolkitClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = toolkit_client_approval.mock_client
    cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
    session = get_oneshot_session(cdf_tool.client)
    assert session.id == 5192234284402249
    assert session.nonce == "QhlCnImCBwBNc72N"
    assert session.status == "READY"
    assert session.type == "ONESHOT_TOKEN_EXCHANGE"


def test_run_transformation(toolkit_client_approval: ApprovalToolkitClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.toolkit_client = toolkit_client_approval.mock_client
    cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
    transformation = Transformation(
        name="Test transformation",
        external_id="test",
        query="SELECT * FROM timeseries",
    )
    toolkit_client_approval.append(Transformation, transformation)

    assert RunTransformationCommand().run_transformation(cdf_tool, "test") is True


@pytest.mark.skip("Needs investigation")
def test_run_function(cdf_tool_mock: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient) -> None:
    function = Function(
        id=1234567890,
        name="test3",
        external_id="fn_test3",
        description="Returns the input data, secrets, and function info.",
        owner="pytest",
        status="RUNNING",
        file_id=1234567890,
        function_path="./handler.py",
        created_time=int(datetime.now().timestamp() / 1000),
        secrets={"my_secret": "***"},
    )
    toolkit_client_approval.append(Function, function)
    toolkit_client_approval.mock_client.functions.call.return_value = FunctionCall(
        id=1234567890,
        status="RUNNING",
        start_time=int(datetime.now().timestamp() / 1000),
    )
    cmd = RunFunctionCommand()

    cmd.run_cdf(
        cdf_tool_mock,
        organization_dir=RUN_DATA,
        build_env_name="dev",
        external_id="fn_test3",
        schedule="daily-8pm-utc",
        wait=False,
    )
    assert toolkit_client_approval.mock_client.functions.call.called


@pytest.mark.skip("Needs investigation")
def test_run_local_function(cdf_tool_mock: CDFToolConfig) -> None:
    cmd = RunFunctionCommand()

    cmd.run_local(
        ToolGlobals=cdf_tool_mock,
        organization_dir=RUN_DATA,
        build_env_name="dev",
        external_id="fn_test3",
        schedule="daily-8pm-utc",
        rebuild_env=False,
    )
