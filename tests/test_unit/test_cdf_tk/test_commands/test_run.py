import os
from collections.abc import Iterator
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from cognite.client.data_classes import ClientCredentials, CreatedSession
from cognite.client.data_classes.functions import Function, FunctionCall
from cognite.client.data_classes.transformations import Transformation, TransformationDestination
from cognite.client.data_classes.workflows import (
    WorkflowExecution,
    WorkflowVersionId,
)
from questionary import Choice
from pydantic import JsonValue
from rich.console import Console

from cognite_toolkit._cdf_tk.client.api.workflow_executions import WorkflowExecutionsAPI
from cognite_toolkit._cdf_tk.client.api.workflow_triggers import WorkflowTriggersAPI
from cognite_toolkit._cdf_tk.client.api.workflow_versions import WorkflowVersionsAPI
from cognite_toolkit._cdf_tk.client.api.workflows import WorkflowsAPI
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    Column,
    SQLQueryResponse,
    TransformationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    NonceCredentials as TransformationNonceCredentials,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_job import (
    TransformationJobMetricResponse,
    TransformationJobResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowVersionId as ToolkitWorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import (
    WorkflowExecutionDetailedResponse,
    WorkflowExecutionResponse,
    WorkflowTaskExecution,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import WorkflowTriggerResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.commands import (
    BuildV2Command,
    RunFunctionCommand,
    RunTransformationCommand,
    RunWorkflowCommand,
    RunWorkflowV2Command,
)
from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildLineage
from cognite_toolkit._cdf_tk.commands.run import FunctionCallArgs, RunTransformationV2Command
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.commands.run import FunctionCallArgs
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from tests.data import RUN_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import MockQuestionary


class TestRunTransformation:
    def test_run_transformation(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        transformation = Transformation(
            id=1,
            name="Test transformation",
            external_id="test",
            query="SELECT * FROM timeseries",
            destination=TransformationDestination.timeseries(),
            conflict_mode="upsert",
            is_public=False,
            ignore_null_fields=False,
            created_time=0,
            last_updated_time=0,
            owner="pytest",
            owner_is_current_user=True,
        )
        toolkit_client_approval.append(Transformation, transformation)

        assert RunTransformationCommand().run_transformation(toolkit_client_approval.mock_client, "test") is True


def _transformation_response(external_id: str, name: str, query: str = "SELECT 1") -> TransformationResponse:
    return TransformationResponse(
        id=abs(hash(external_id)) % 10_000,
        external_id=external_id,
        name=name,
        ignore_null_fields=True,
        created_time=1,
        last_updated_time=1,
        query=query,
        is_public=True,
        conflict_mode="upsert",
        destination={"type": "assets"},
        owner="test",
        owner_is_current_user=True,
        has_source_oidc_credentials=False,
        has_destination_oidc_credentials=False,
    )


def _transformation_job(status: str, external_id: str = "tr_assets") -> TransformationJobResponse:
    return TransformationJobResponse(
        id=1,
        uuid="job-uuid",
        status=status,
        transformation_id=1,
        transformation_external_id=external_id,
        source_project="my-project",
        destination_project="my-project",
        destination={"type": "assets"},
        conflict_mode="upsert",
        query="SELECT 1",
        ignore_null_fields=True,
        created_time=1,
    )


class TestRunTransformationV2:
    @staticmethod
    def _mock_client() -> MagicMock:
        client = MagicMock()
        client.config.project = "my-project"
        client.iam.sessions.create.return_value = CreatedSession(id=42, status="READY", nonce="dummy-nonce")
        client.tool.transformations.run.return_value = _transformation_job("Created")
        return client

    def test_run_transformation(self) -> None:
        client = self._mock_client()

        result = RunTransformationV2Command().run_transformation(client, "tr_assets", is_dry_run=False, wait=False)

        assert result is True
        client.tool.transformations.run.assert_called_once_with(
            ExternalId(external_id="tr_assets"),
            nonce=TransformationNonceCredentials(session_id=42, nonce="dummy-nonce", cdf_project_name="my-project"),
        )

    def test_run_transformation_dry_run(self) -> None:
        client = self._mock_client()
        client.tool.transformations.retrieve.return_value = [
            _transformation_response("tr_assets", "Assets", query="SELECT * FROM assets")
        ]
        client.tool.transformations.run_query_preview.return_value = SQLQueryResponse(
            schema_=[Column(name="id", sql_type="INT", type="INT", nullable=False)],
            results=[{"id": 1}],
        )

        result = RunTransformationV2Command().run_transformation(client, "tr_assets", is_dry_run=True, wait=False)

        assert result is True
        client.tool.transformations.run.assert_not_called()
        client.tool.transformations.run_query_preview.assert_called_once_with(
            "SELECT * FROM assets", convert_to_string=False
        )

    @patch("cognite_toolkit._cdf_tk.commands.run.time.sleep")
    def test_run_transformation_wait(self, _sleep: MagicMock) -> None:
        client = self._mock_client()
        client.tool.transformations.jobs.retrieve.side_effect = [
            [_transformation_job("Running")],
            [_transformation_job("Completed")],
        ]
        client.tool.transformations.jobs.list_metrics.return_value = [
            TransformationJobMetricResponse(timestamp=1_622_547_800_000, name="assets.read", count=10),
        ]

        result = RunTransformationV2Command().run_transformation(client, "tr_assets", is_dry_run=False, wait=True)

        assert result is True
        client.tool.transformations.jobs.list_metrics.assert_called_once_with(1)

    def test_run_transformation_interactive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transformations = [
            _transformation_response("tr_assets", "Assets"),
            _transformation_response("tr_events", "Events"),
            _transformation_response("tr_files", "Files"),
        ]

        def select_transformation(choices: list[Choice]) -> str:
            assert len(choices) == 3
            return choices[1].value

        answers = [select_transformation, False, False]
        with MockQuestionary(RunTransformationV2Command.__module__, monkeypatch, answers):
            client = self._mock_client()
            client.tool.transformations.list.return_value = transformations

            result = RunTransformationV2Command().run_transformation(client, None, is_dry_run=False, wait=False)

        assert result is True
        client.tool.transformations.list.assert_called_once_with(limit=None)
        client.tool.transformations.run.assert_called_once_with(
            ExternalId(external_id="tr_events"),
            nonce=TransformationNonceCredentials(session_id=42, nonce="dummy-nonce", cdf_project_name="my-project"),
        )

    def test_run_transformation_interactive_no_transformations(self) -> None:
        client = self._mock_client()
        client.tool.transformations.list.return_value = []

        with pytest.raises(ToolkitMissingResourceError, match="No transformations found"):
            RunTransformationV2Command().run_transformation(client, None, is_dry_run=False, wait=False)


@pytest.fixture(scope="session")
def function_build_folder() -> BuildLineage:
    return BuildV2Command(print_warning=False, silent=True).tmp_build(
        RUN_DATA,
        RUN_DATA / "config.dev.yaml",
    )


@pytest.fixture
def mock_function_venv() -> Iterator[MagicMock]:
    with patch("cognite_toolkit._cdf_tk.commands._virtual_env.FunctionVirtualEnvironment") as mock_cls:
        yield mock_cls.return_value


class TestRunFunction:
    def test_run_function_live(
        self, toolkit_client_approval: ApprovalToolkitClient, env_vars_with_client: EnvironmentVariables
    ) -> None:
        function = Function(
            id=1234567890,
            created_time=1_700_000_000_000,
            name="test3",
            external_id="fn_test3",
            description="Returns the input data, secrets, and function info.",
            owner="pytest",
            status="RUNNING",
            file_id=1234567890,
            function_path="./handler.py",
            secrets={"my_secret": "***"},
        )
        toolkit_client_approval.append(Function, function)
        toolkit_client_approval.mock_client.functions.call.return_value = FunctionCall(
            id=1234567890,
            status="RUNNING",
            start_time=int(datetime.now().timestamp() / 1000),
            function_id=1234567890,
        )
        cmd = RunFunctionCommand()

        cmd.run_cdf(
            env_vars_with_client,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="fn_test3",
            data_source="daily-8pm-utc",
            wait=False,
            config_yaml=RUN_DATA / "config.dev.yaml",
        )
        assert toolkit_client_approval.mock_client.functions.call.called

    @patch.dict(
        os.environ,
        {
            "IDP_FUN_CLIENT_ID": "dummy",
            "IDP_FUN_CLIENT_SECRET": "dummy",
        },
    )
    def test_run_local_function(
        self, env_vars_with_client_cheap: EnvironmentVariables, mock_function_venv: MagicMock
    ) -> None:
        cmd = RunFunctionCommand()

        cmd.run_local(
            env_vars=env_vars_with_client_cheap,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="fn_test3",
            data_source="daily-8pm-utc",
            rebuild_env=False,
            config_yaml=RUN_DATA / "config.dev.yaml",
            virtual_env_folder_name="function_local_venvs_test_run_local_function",
        )

        mock_function_venv.create.assert_called_once()
        assert mock_function_venv.execute.call_count == 2

    @patch.dict(
        os.environ,
        {
            "IDP_WF_CLIENT_ID": "dummy",
            "IDP_WF_CLIENT_SECRET": "dummy",
        },
    )
    def test_run_local_function_with_workflow(
        self, env_vars_with_client_cheap: EnvironmentVariables, mock_function_venv: MagicMock
    ) -> None:
        cmd = RunFunctionCommand()

        cmd.run_local(
            env_vars=env_vars_with_client_cheap,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="fn_test3",
            data_source="workflow",
            rebuild_env=False,
            config_yaml=RUN_DATA / "config.dev.yaml",
            virtual_env_folder_name="function_local_venvs_test_run_local_function_workflow",
        )

        mock_function_venv.create.assert_called_once()
        assert mock_function_venv.execute.call_count == 2

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
        self, data_source: str, expected: FunctionCallArgs, function_build_folder: BuildLineage
    ) -> None:
        environment_variables = {
            expected.client_id_env_name: expected.authentication.client_id,
            expected.client_secret_env_name: expected.authentication.client_secret,
        }
        actual = RunFunctionCommand._get_call_args(
            data_source, "fn_test3", function_build_folder, environment_variables, is_interactive=False
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
                config_yaml=RUN_DATA / "config.dev.yaml",
            )
            is True
        )

    @patch("cognite_toolkit._cdf_tk.commands.run.time.sleep")
    def test_run_workflow_wait_for_completion(
        self,
        _sleep: MagicMock,
        toolkit_client_approval: ApprovalToolkitClient,
        env_vars_with_client: EnvironmentVariables,
    ) -> None:
        toolkit_client_approval.mock_client.workflows.executions.run.return_value = WorkflowExecution(
            id="1234567890",
            workflow_external_id="workflow",
            status="running",
            created_time=int(datetime.now().timestamp() / 1000),
            version="v1",
        )
        wf_task = MagicMock()
        wf_task.timeout = 60
        wf_task.retries = 1
        wf_version = MagicMock()
        wf_version.workflow_definition.tasks = [wf_task]
        toolkit_client_approval.mock_client.workflows.versions.retrieve = MagicMock(return_value=wf_version)

        now_ms = int(datetime.now().timestamp() * 1000)
        running = MagicMock()
        running.status = "running"
        running.executed_tasks = [
            SimpleNamespace(
                status="in_progress",
                external_id="t1",
                start_time=now_ms,
                end_time=None,
                reason_for_incompletion=None,
            )
        ]
        done = MagicMock()
        done.status = "completed"
        done.executed_tasks = [
            SimpleNamespace(
                status="completed",
                external_id="t1",
                start_time=now_ms,
                end_time=now_ms + 1000,
                reason_for_incompletion=None,
            )
        ]
        toolkit_client_approval.mock_client.workflows.executions.retrieve_detailed.side_effect = [running, done]

        assert (
            RunWorkflowCommand().run_workflow(
                env_vars_with_client,
                organization_dir=RUN_DATA,
                build_env_name="dev",
                external_id="workflow",
                version="v1",
                wait=True,
                config_yaml=RUN_DATA / "config.dev.yaml",
            )
            is True
        )
        assert toolkit_client_approval.mock_client.workflows.executions.retrieve_detailed.call_count == 2

    def test_run_workflow_wait_passes_sdk_workflow_version_id(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        env_vars_with_client: EnvironmentVariables,
    ) -> None:
        """Regression test: retrieve must be called with the SDK's WorkflowVersionId,
        not the toolkit's internal WorkflowVersionId, otherwise WorkflowIds.load raises
        ValueError: Invalid input to WorkflowIds.load."""
        toolkit_client_approval.mock_client.workflows.executions.run.return_value = WorkflowExecution(
            id="1234567890",
            workflow_external_id="workflow",
            status="running",
            created_time=int(datetime.now().timestamp() / 1000),
            version="v1",
        )
        wf_task = MagicMock()
        wf_task.timeout = 60
        wf_task.retries = 1
        wf_version = MagicMock()
        wf_version.workflow_definition.tasks = [wf_task]
        retrieve_mock = MagicMock(return_value=wf_version)
        toolkit_client_approval.mock_client.workflows.versions.retrieve = retrieve_mock
        toolkit_client_approval.mock_client.workflows.executions.retrieve_detailed.return_value = MagicMock(
            status="completed", executed_tasks=[]
        )

        RunWorkflowCommand().run_workflow(
            env_vars_with_client,
            organization_dir=RUN_DATA,
            build_env_name="dev",
            external_id="workflow",
            version="v1",
            wait=True,
            config_yaml=RUN_DATA / "config.dev.yaml",
        )

        retrieve_mock.assert_called_once()
        (called_with,) = retrieve_mock.call_args.args
        assert isinstance(called_with, WorkflowVersionId), (
            f"retrieve() must receive the SDK's WorkflowVersionId, got {type(called_with).__qualname__}"
        )
        assert called_with.workflow_external_id == "workflow"
        assert called_with.version == "v1"


def _workflow_version(
    external_id: str = "workflow", version: str = "v1", last_updated_time: int = 0
) -> WorkflowVersionResponse:
    return WorkflowVersionResponse.model_validate(
        {
            "workflowExternalId": external_id,
            "version": version,
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "task1",
                        "type": "function",
                        "parameters": {"function": {"externalId": "fn_test3"}},
                        "timeout": 60,
                        "retries": 1,
                    }
                ]
            },
            "createdTime": 0,
            "lastUpdatedTime": last_updated_time,
        }
    )


def _workflow_execution(status: str = "RUNNING") -> WorkflowExecutionResponse:
    return WorkflowExecutionResponse.model_validate(
        {
            "id": "1234567890",
            "workflowExternalId": "workflow",
            "version": "v1",
            "status": status,
            "createdTime": int(datetime.now().timestamp() * 1000),
        }
    )


def _task_execution(
    id_: str,
    external_id: str,
    status: str,
    output: JsonValue = None,
    reason_for_incompletion: str | None = None,
) -> WorkflowTaskExecution:
    now_ms = int(datetime.now().timestamp() * 1000)
    return WorkflowTaskExecution.model_validate(
        {
            "id": id_,
            "externalId": external_id,
            "status": status,
            "startTime": now_ms,
            "endTime": None if status in ("SCHEDULED", "IN_PROGRESS") else now_ms + 1000,
            "output": output,
            "reasonForIncompletion": reason_for_incompletion,
        }
    )


def _detailed_execution(status: str, executed_tasks: list[WorkflowTaskExecution]) -> WorkflowExecutionDetailedResponse:
    return WorkflowExecutionDetailedResponse.model_validate(
        {
            "id": "1234567890",
            "workflowExternalId": "workflow",
            "version": "v1",
            "status": status,
            "createdTime": int(datetime.now().timestamp() * 1000),
            "workflowDefinition": _workflow_version().workflow_definition.dump(),
            "executedTasks": executed_tasks,
        }
    )


@pytest.fixture
def workflow_client() -> MagicMock:
    """Ad-hoc mock of the parts of the ToolkitClient that RunWorkflowV2Command uses."""
    client = MagicMock()
    # The command passes the console to rich.Progress, which does not accept a mock.
    client.console = Console()
    client.iam.sessions.create.return_value.nonce = "dummy-nonce"
    client.tool.workflows = MagicMock(spec=WorkflowsAPI)
    client.tool.workflows.versions = MagicMock(spec_set=WorkflowVersionsAPI)
    client.tool.workflows.triggers = MagicMock(spec_set=WorkflowTriggersAPI)
    client.tool.workflows.executions = MagicMock(spec_set=WorkflowExecutionsAPI)
    client.tool.workflows.executions.run.return_value = _workflow_execution()
    return client


class TestRunWorkflowV2:
    def test_run_workflow(self, workflow_client: MagicMock) -> None:
        client = workflow_client
        client.tool.workflows.versions.retrieve.return_value = [_workflow_version()]

        assert (
            RunWorkflowV2Command().run_workflow(
                client,
                external_id="workflow",
                version="v1",
                wait=False,
            )
            is True
        )
        client.tool.workflows.executions.run.assert_called_once_with(
            ToolkitWorkflowVersionId(workflow_external_id="workflow", version="v1"),
            nonce="dummy-nonce",
            input=None,
        )

    def test_run_workflow_without_version_runs_last_updated(self, workflow_client: MagicMock) -> None:
        client = workflow_client
        client.tool.workflows.versions.list.return_value = [
            _workflow_version(version="v1", last_updated_time=1),
            _workflow_version(version="v2", last_updated_time=2),
        ]

        RunWorkflowV2Command().run_workflow(
            client,
            external_id="workflow",
            version=None,
            wait=False,
        )

        client.tool.workflows.executions.run.assert_called_once_with(
            ToolkitWorkflowVersionId(workflow_external_id="workflow", version="v2"),
            nonce="dummy-nonce",
            input=None,
        )

    @patch("cognite_toolkit._cdf_tk.commands.run.questionary.confirm")
    def test_run_workflow_interactive_uses_trigger_input(self, confirm: MagicMock, workflow_client: MagicMock) -> None:
        # The first confirm is whether to use the trigger input, the second whether to wait.
        confirm.return_value.unsafe_ask.side_effect = [True, False]
        trigger = WorkflowTriggerResponse.model_validate(
            {
                "externalId": "my_trigger",
                "triggerRule": {"triggerType": "schedule", "cronExpression": "0 0 * * *"},
                "workflowExternalId": "workflow",
                "workflowVersion": "v1",
                "input": {"breakfast": "egg and bacon"},
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "isPaused": False,
            }
        )
        client = workflow_client
        client.tool.workflows.versions.list.return_value = [_workflow_version()]
        client.tool.workflows.triggers.list.return_value = [trigger]

        RunWorkflowV2Command().run_workflow(
            client,
            external_id=None,
            version=None,
            wait=False,
        )

        client.tool.workflows.executions.run.assert_called_once_with(
            ToolkitWorkflowVersionId(workflow_external_id="workflow", version="v1"),
            nonce="dummy-nonce",
            input={"breakfast": "egg and bacon"},
        )

    @patch("cognite_toolkit._cdf_tk.commands.run.time.sleep")
    def test_run_workflow_wait_for_completion(self, _sleep: MagicMock, workflow_client: MagicMock) -> None:
        client = workflow_client
        client.tool.workflows.versions.retrieve.return_value = [_workflow_version()]
        client.tool.workflows.executions.retrieve.side_effect = [
            [_detailed_execution("RUNNING", [_task_execution("1", "task1", "IN_PROGRESS")])],
            [_detailed_execution("COMPLETED", [_task_execution("1", "task1", "COMPLETED")])],
        ]

        assert (
            RunWorkflowV2Command().run_workflow(
                client,
                external_id="workflow",
                version="v1",
                wait=True,
            )
            is True
        )
        assert client.tool.workflows.executions.retrieve.call_count == 2

    def test_run_workflow_missing_version(self, workflow_client: MagicMock) -> None:
        client = workflow_client
        client.tool.workflows.versions.retrieve.return_value = []

        with pytest.raises(ToolkitMissingResourceError, match="Could not find workflow"):
            RunWorkflowV2Command().run_workflow(
                client,
                external_id="workflow",
                version="v2",
                wait=False,
            )

    def test_finished_task_count_counts_tasks_not_attempts(self) -> None:
        """A retried task has one execution per attempt, but is still a single task."""
        execution = _detailed_execution(
            "RUNNING",
            [
                _task_execution("1", "task1", "FAILED"),
                _task_execution("2", "task1", "COMPLETED"),
                _task_execution("3", "task2", "IN_PROGRESS"),
            ],
        )

        assert RunWorkflowV2Command._finished_task_count(execution) == 1

    def test_report_attempt_reports_failure_and_retry_once(self) -> None:
        progress = MagicMock()
        reported: set[str] = set()
        failed = _task_execution("1", "task1", "FAILED", reason_for_incompletion="boom")
        retry = _task_execution("2", "task1", "IN_PROGRESS")

        RunWorkflowV2Command._report_attempt(progress, failed, 1, reported)
        RunWorkflowV2Command._report_attempt(progress, retry, 2, reported)
        # The same attempts are seen again on the next poll, and must not be reported twice.
        RunWorkflowV2Command._report_attempt(progress, failed, 1, reported)
        RunWorkflowV2Command._report_attempt(progress, retry, 2, reported)

        assert [call.args[0] for call in progress.console.print.call_args_list] == [
            "[red]Task 'task1' attempt 1 FAILED[/]: boom",
            "[yellow]Retrying task 'task1' (attempt 2)[/]",
        ]

    @patch("cognite_toolkit._cdf_tk.commands.run.print")
    def test_print_tasks_groups_attempts_and_truncates_output(self, print_: MagicMock) -> None:
        execution = _detailed_execution(
            "COMPLETED",
            [
                _task_execution("1", "task1", "FAILED", reason_for_incompletion="boom"),
                _task_execution("2", "task1", "COMPLETED", output={"result": "ok"}),
                _task_execution("3", "task2", "COMPLETED", output="a" * 60),
            ],
        )

        RunWorkflowV2Command._print_tasks(
            ToolkitWorkflowVersionId(workflow_external_id="workflow", version="v1"), execution
        )

        table = print_.call_args.args[0]
        rendered = {column.header: list(column._cells) for column in table.columns}
        assert {key: rendered[key] for key in ("Task", "Attempt", "Status", "Output")} == {
            "Task": ["task1 (2 attempts)", "", "task2"],
            "Attempt": ["1/2", "2/2", "1"],
            "Status": ["FAILED", "COMPLETED", "COMPLETED"],
            "Output": ["", '{"result": "ok"}', f"{'a' * 49}…"],
        }
