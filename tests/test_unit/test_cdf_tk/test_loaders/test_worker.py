import contextlib
import io
from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes.workflows import WorkflowScheduledTriggerRule, WorkflowTrigger

from cognite_toolkit._cdf_tk.loaders import ResourceWorker, WorkflowTriggerLoader
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
                    WorkflowTriggerLoader._MetadataKey.secret_hash: "outdated-hash",
                },
            ),
        )
        loader = WorkflowTriggerLoader.create_loader(toolkit_client_approval.mock_client)

        worker = ResourceWorker(loader)
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
            _ = worker.load_resources(
                [local_file], return_existing=False, environment_variables={}, is_dry_run=False, verbose=True
            )

        terminal_output = output_capture.getvalue()
        assert "my_super_secret_42" not in terminal_output
