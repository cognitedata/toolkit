import io
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, patch

from rich.console import Console

from cognite_toolkit._cdf_tk.client.resource_classes.group import DataSetScope, FilesAcl, FunctionsAcl
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    ScheduleTriggerRule,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import ReadResource
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO, WorkflowTriggerIO
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDeployV2CommandCategorizeResources:
    def test_mask_sensitive_data(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        toolkit_client_approval.append(
            WorkflowTriggerResponse,
            WorkflowTriggerResponse(
                external_id="my_trigger",
                trigger_rule=ScheduleTriggerRule(cron_expression="* * * * *"),
                workflow_external_id="my_workflow",
                workflow_version="v1",
                created_time=0,
                last_updated_time=0,
                is_paused=False,
                metadata={
                    WorkflowTriggerIO._MetadataKey.secret_hash: "outdated-hash",
                },
            ),
        )
        loader = WorkflowTriggerIO.create_loader(toolkit_client_approval.mock_client)

        local_file = MagicMock(spec=Path)
        local_file.read_text.return_value = """externalId: my_trigger
triggerRule:
  triggerType: schedule
  cronExpression: '* * * * *'
workflowExternalId: my_workflow
workflowVersion: v1
authentication:
  clientId: my_client_id
  clientSecret: my_super_secret_42
"""
        resource_dict = loader.load_resource_file(local_file, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(deepcopy(resource_dict[0]))
        resource_id = loader.get_id(resource)
        existing_list = loader.retrieve([resource_id])
        output_capture = io.StringIO()
        console = Console(file=output_capture, highlight=False, color_system=None)
        _ = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [local_file])},
            cdf_by_id={resource_id: existing_list[0]},
            console=console,
            options=DeployOptions(verbose=True),
        )

        terminal_output = output_capture.getvalue()
        assert "my_super_secret_42" not in terminal_output

    def test_worker_uses_function_capabilities(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        # This test verifies that function-specific capabilities are used
        # for FunctionLoader rather than generic capabilities
        with patch(
            "cognite_toolkit._cdf_tk.resource_ios._resource_ios.function.FunctionIO.load_resource_file"
        ) as mock_load_resource_file:
            mock_authorization = toolkit_client_approval.mock_client.tool.token.verify_acls
            mock_authorization.return_value = []
            mock_load_resource_file.return_value = [
                {
                    "externalId": "my_function",
                    "name": "My Function",
                    "fileId": 123,
                    "dataSetExternalId": "my_dataset",
                }
            ]
            loader = FunctionIO.create_loader(toolkit_client_approval.mock_client, None)

            local_file = MagicMock(spec=Path)
            local_file.parent.name = FunctionIO.folder_name

            resource_dicts = loader.load_resource_file(local_file, None)
            resource = loader.load_resource(deepcopy(resource_dicts[0]))
            DeployV2Command._validate_access(loader, [resource], is_dry_run=False)
            assert mock_authorization.call_count >= 1

            capabilities_arg = mock_authorization.call_args_list[0].args[0]
            assert len(capabilities_arg) == 2
            assert isinstance(capabilities_arg[0], FunctionsAcl)
            assert isinstance(capabilities_arg[1], FilesAcl)
            assert isinstance(capabilities_arg[1].scope, DataSetScope)
            assert capabilities_arg[1].scope.ids == [4228768136987700990]
