from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ClientCredentials, CreatedSession

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    ScheduleTriggerRule,
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    SubworkflowTaskParameters,
    Task,
    WorkflowDefinition,
    WorkflowVersionRequest,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios import WorkflowTriggerIO, WorkflowVersionIO
from cognite_toolkit._cdf_tk.utils import calculate_secure_hash


class TestWorkflowTriggerLoader:
    def test_credentials_missing_raise(self) -> None:
        trigger_content = """externalId: daily-8am-utc
triggerRule:
    triggerType: schedule
    cronExpression: 0 8 * * *
workflowExternalId: wf_example_repeater
workflowVersion: v1
        """
        trigger_file = MagicMock(spec=Path)
        trigger_file.read_text.return_value = trigger_content

        config = MagicMock(spec=ToolkitClientConfig)
        config.is_strict_validation = True
        config.credentials = OAuthClientCredentials(
            client_id="toolkit-client-id",
            client_secret="toolkit-client-secret",
            token_url="https://cognite.com/token",
            scopes=["USER_IMPERSONATION"],
        )
        with monkeypatch_toolkit_client() as client:
            client.config = config
            loader = WorkflowTriggerIO.create_loader(client)

        with pytest.raises(ToolkitRequiredValueError):
            loader.load_resource_file(trigger_file, {})
        client.config.is_strict_validation = False
        local_dict = loader.load_resource_file(trigger_file, {})[0]
        credentials = loader._authentication_by_id[loader.get_id(local_dict).external_id]
        assert credentials.client_id == "toolkit-client-id"
        assert credentials.client_secret == "toolkit-client-secret"

    def test_credentials_unchanged_changed(self) -> None:
        local_content = """externalId: daily-8am-utc
triggerRule:
  triggerType: schedule
  cronExpression: 0 8 * * *
workflowExternalId: wf_example_repeater
workflowVersion: v1
authentication:
  clientId: my-client-id
  clientSecret: my-client-secret
"""

        cdf_trigger = WorkflowTriggerResponse(
            external_id="daily-8am-utc",
            trigger_rule=ScheduleTriggerRule(cron_expression="0 8 * * *"),
            workflow_external_id="wf_example_repeater",
            workflow_version="v1",
            created_time=0,
            last_updated_time=0,
            is_paused=False,
            metadata={
                WorkflowTriggerIO._MetadataKey.secret_hash: calculate_secure_hash(
                    {"clientId": "my-client-id", "clientSecret": "my-client-secret"}, shorten=True
                )
            },
        )
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowTriggerIO(client, None, None)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_content
        local_dumped = loader.load_resource_file(filepath, {})[0]
        cdf_dumped = loader.dump_resource(cdf_trigger, local_dumped)

        assert cdf_dumped == local_dumped

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_content.replace("my-client-secret", "my-client-secret-changed")
        local_dumped = loader.load_resource_file(filepath, {})[0]
        cdf_dumped = loader.dump_resource(cdf_trigger, local_dumped)

        assert cdf_dumped != local_dumped

    def test_create_uses_client_credentials_session(self) -> None:
        credentials = ClientCredentials(client_id="my-client-id", client_secret="my-client-secret")
        trigger = WorkflowTriggerRequest(
            external_id="daily-8am-utc",
            trigger_rule=ScheduleTriggerRule(cron_expression="0 8 * * *"),
            workflow_external_id="wf_example_repeater",
            workflow_version="v1",
        )
        with monkeypatch_toolkit_client() as client:
            client.iam.sessions.create.return_value = CreatedSession(123, "READY", "my-nonce")
            loader = WorkflowTriggerIO(client, None, None)
            loader._authentication_by_id["daily-8am-utc"] = credentials
            client.tool.workflows.triggers.create.return_value = [
                WorkflowTriggerResponse(
                    external_id="daily-8am-utc",
                    trigger_rule=trigger.trigger_rule,
                    workflow_external_id="wf_example_repeater",
                    workflow_version="v1",
                    created_time=0,
                    last_updated_time=0,
                    is_paused=False,
                )
            ]
            loader.create([trigger])

        client.iam.sessions.create.assert_called_once_with(credentials, session_type="CLIENT_CREDENTIALS")


class TestWorkflowVersionLoader:
    def test_topological_sort_raises_on_cycle(self) -> None:
        dependencies = {
            "a": "b",
            "b": "c",
            "c": "a",  # This creates a cycle
        }

        workflows = [
            WorkflowVersionRequest(
                workflow_external_id=id_,
                version="v1",
                workflow_definition=WorkflowDefinition(
                    tasks=[
                        Task(
                            external_id=f"task_{id_}",
                            type="subworkflow",
                            parameters=SubworkflowTaskParameters(
                                subworkflow=WorkflowVersionId(workflow_external_id=dependency, version="v1")
                            ),
                        ),
                    ]
                ),
            )
            for id_, dependency in dependencies.items()
        ]

        with pytest.raises(ToolkitCycleError) as exc:
            WorkflowVersionIO.topological_sort(workflows)

        error = exc.value
        assert isinstance(error, ToolkitCycleError)
        assert error.args[1] == [
            WorkflowVersionId(workflow_external_id=id_, version="v1") for id_ in ["a", "c", "b", "a"]
        ]
