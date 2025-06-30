import os
from datetime import datetime
from unittest.mock import patch

import pytest
from cognite.client.data_classes import ClientCredentials
from cognite.client.data_classes.functions import Function, FunctionCall
from cognite.client.data_classes.transformations import Transformation
from cognite.client.data_classes.workflows import (
    WorkflowExecution,
)

from cognite_toolkit._cdf_tk.commands import RunFunctionCommand, RunTransformationCommand, RunWorkflowCommand
from cognite_toolkit._cdf_tk.commands.run import FunctionCallArgs
from cognite_toolkit._cdf_tk.data_classes import ModuleResources
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import RUN_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestRunTransformation:
    def test_run_transformation(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        transformation = Transformation(
            name="Test transformation",
            external_id="test",
            query="SELECT * FROM timeseries",
        )
        toolkit_client_approval.append(Transformation, transformation)

        assert RunTransformationCommand().run_transformation(toolkit_client_approval.mock_client, "test") is True


@pytest.fixture(scope="session")
def functon_module_resources() -> ModuleResources:
    return ModuleResources(RUN_DATA, "dev")


class TestRunFunction:
    def test_run_function_live(
        self, toolkit_client_approval: ApprovalToolkitClient, env_vars_with_client: EnvironmentVariables
    ) -> None:
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
            env_vars_with_client,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="fn_test3",
            data_source="daily-8pm-utc",
            wait=False,
        )
        assert toolkit_client_approval.mock_client.functions.call.called

    @patch.dict(
        os.environ,
        {
            "IDP_FUN_CLIENT_ID": "dummy",
            "IDP_FUN_CLIENT_SECRET": "dummy",
        },
    )
    def test_run_local_function(self, env_vars_with_client: EnvironmentVariables) -> None:
        cmd = RunFunctionCommand()

        cmd.run_local(
            env_vars=env_vars_with_client,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="fn_test3",
            data_source="daily-8pm-utc",
            rebuild_env=False,
            virtual_env_folder_name="function_local_venvs_test_run_local_function",
        )

    @patch.dict(
        os.environ,
        {
            "IDP_WF_CLIENT_ID": "dummy",
            "IDP_WF_CLIENT_SECRET": "dummy",
        },
    )
    def test_run_local_function_with_workflow(self, env_vars_with_client: EnvironmentVariables) -> None:
        cmd = RunFunctionCommand()

        cmd.run_local(
            env_vars=env_vars_with_client,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="fn_test3",
            data_source="workflow",
            rebuild_env=False,
            virtual_env_folder_name="function_local_venvs_test_run_local_function_workflow",
        )

    @pytest.mark.parametrize(
        "data_source, expected",
        [
            pytest.param(
                "workflow",
                FunctionCallArgs(
                    data={
                        "breakfast": "today: egg and bacon",
                        "lunch": "today: a chicken",
                        "dinner": "today: steak with stakes on the side",
                    },
                    authentication=ClientCredentials(
                        client_id="workflow_client_id",
                        client_secret="workflow_client_secret",
                    ),
                    client_id_env_name="IDP_WF_CLIENT_ID",
                    client_secret_env_name="IDP_WF_CLIENT_SECRET",
                ),
                id="workflow",
            ),
            pytest.param(
                "daily-8am-utc",
                FunctionCallArgs(
                    data={
                        "breakfast": "today: peanut butter sandwich and coffee",
                        "lunch": "today: greek salad and water",
                        "dinner": "today: steak and red wine",
                    },
                    authentication=ClientCredentials(
                        client_id="function_client_id",
                        client_secret="function_client_secret",
                    ),
                    client_id_env_name="IDP_FUN_CLIENT_ID",
                    client_secret_env_name="IDP_FUN_CLIENT_SECRET",
                ),
                id="daily-8pm-utc",
            ),
        ],
    )
    def test_get_call_args(
        self, data_source: str, expected: FunctionCallArgs, functon_module_resources: ModuleResources
    ) -> None:
        environment_variables = {
            expected.client_id_env_name: expected.authentication.client_id,
            expected.client_secret_env_name: expected.authentication.client_secret,
        }
        actual = RunFunctionCommand._get_call_args(
            data_source, "fn_test3", functon_module_resources, environment_variables, is_interactive=False
        )

        assert actual == expected


class TestRunWorkflow:
    def test_run_workflow(
        self, toolkit_client_approval: ApprovalToolkitClient, env_vars_with_client: EnvironmentVariables
    ):
        toolkit_client_approval.mock_client.workflows.executions.run.return_value = WorkflowExecution(
            id="1234567890",
            workflow_external_id="workflow",
            status="running",
            created_time=int(datetime.now().timestamp() / 1000),
            version="v1",
        )

        assert (
            RunWorkflowCommand().run_workflow(
                env_vars_with_client,
                organization_dir=RUN_DATA,
                build_env_name="dev",
                external_id="workflow",
                version="v1",
                wait=False,
            )
            is True
        )
