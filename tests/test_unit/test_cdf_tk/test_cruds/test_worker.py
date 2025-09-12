import contextlib
import io
from pathlib import Path
from unittest.mock import MagicMock, patch

from cognite.client.data_classes.capabilities import FilesAcl, FunctionsAcl
from cognite.client.data_classes.workflows import WorkflowScheduledTriggerRule, WorkflowTrigger

from cognite_toolkit._cdf_tk.cruds import FunctionCRUD, ResourceWorker, WorkflowTriggerCRUD
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestResourceWorker:
    def test_mask_sensitive_data(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        toolkit_client_approval.append(
            WorkflowTrigger,
            WorkflowTrigger(
                "my_trigger",
                WorkflowScheduledTriggerRule(cron_expression="* * * * *"),
                "my_workflow",
                "v1",
                metadata={
                    WorkflowTriggerCRUD._MetadataKey.secret_hash: "outdated-hash",
                },
            ),
        )
        loader = WorkflowTriggerCRUD.create_loader(toolkit_client_approval.mock_client)

        worker = ResourceWorker(loader, "deploy")
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
        output_capture = io.StringIO()
        with contextlib.redirect_stdout(output_capture):
            _ = worker.prepare_resources([local_file], environment_variables={}, is_dry_run=False, verbose=True)

        terminal_output = output_capture.getvalue()
        assert "my_super_secret_42" not in terminal_output

    def test_worker_uses_function_capabilities(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        # This test verifies that the ResourceWorker uses function-specific capabilities
        # for FunctionLoader rather than generic capabilities
        with patch(
            "cognite_toolkit._cdf_tk.cruds._resource_cruds.function.FunctionCRUD.load_resource_file"
        ) as mock_load_resource_file:
            mock_authorization = toolkit_client_approval.mock_client.verify.authorization
            mock_authorization.return_value = []
            mock_load_resource_file.return_value = [
                {
                    "externalId": "my_function",
                    "name": "My Function",
                    "fileId": 123,
                }
            ]

            loader = FunctionCRUD.create_loader(toolkit_client_approval.mock_client, None)
            loader.data_set_id_by_external_id = {"my_function": 789}

            local_file = MagicMock(spec=Path)
            local_file.parent.name = FunctionCRUD.folder_name

            worker = ResourceWorker(loader, "deploy")
            local_by_id = worker.load_resources([local_file], None, False)
            worker.validate_access(local_by_id, is_dry_run=False)
            mock_authorization.assert_called_once()

            capabilities_arg = mock_authorization.call_args[0][0]
            assert len(capabilities_arg) == 2
            assert isinstance(capabilities_arg[0], FunctionsAcl)
            assert isinstance(capabilities_arg[1], FilesAcl)
            assert isinstance(capabilities_arg[1].scope, FilesAcl.Scope.DataSet)
            assert capabilities_arg[1].scope.ids == [789]
