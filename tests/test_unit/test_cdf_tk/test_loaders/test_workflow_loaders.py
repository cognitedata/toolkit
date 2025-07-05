from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import WorkflowTrigger, WorkflowVersionUpsert, WorkflowVersionUpsertList
from cognite.client.data_classes.workflows import WorkflowScheduledTriggerRule, WorkflowVersionId

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.loaders import WorkflowTriggerLoader, WorkflowVersionLoader
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
            loader = WorkflowTriggerLoader.create_loader(client)

        with pytest.raises(ToolkitRequiredValueError):
            loader.load_resource_file(trigger_file, {})
        client.config.is_strict_validation = False
        local_dict = loader.load_resource_file(trigger_file, {})[0]
        credentials = loader._authentication_by_id[loader.get_id(local_dict)]
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

        cdf_trigger = WorkflowTrigger(
            "daily-8am-utc",
            trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 8 * * *"),
            workflow_external_id="wf_example_repeater",
            workflow_version="v1",
            metadata={
                WorkflowTriggerLoader._MetadataKey.secret_hash: calculate_secure_hash(
                    {"clientId": "my-client-id", "clientSecret": "my-client-secret"}, shorten=True
                )
            },
        )
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowTriggerLoader(client, None, None)

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


class TestWorkflowVersionLoader:
    def test_retrieve_above_chunk_limit(self) -> None:
        filter_limit = 100
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader.create_loader(client)
        ids = [WorkflowVersionId(f"my_workflow_{no}", "v1") for no in range(filter_limit + 1)]

        _ = loader.retrieve(ids)

        assert client.workflows.versions.list.call_count == 2
        call_above_limit = [
            call_no
            for call_no, call in enumerate(client.workflows.versions.list.call_args_list, 1)
            if len(call.args[0]) > filter_limit
        ]
        assert not call_above_limit, "Above limit should not be called"

    def test_topological_sort_empty_list(self) -> None:
        """Test that empty lists are handled correctly by topological sort."""
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader(client, None, None)

            result = loader._topological_sort_workflow_versions(WorkflowVersionUpsertList([]))

            assert len(result) == 0

    def test_topological_sort_no_dependencies(self) -> None:
        """Test sorting workflow versions without dependencies."""
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader(client, None, None)

            # Create workflow version without dependencies
            version_dict = {
                "workflowExternalId": "simple_workflow",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "simple_task",
                            "type": "function",
                            "parameters": {
                                "function": {
                                    "externalId": "some_function"
                                }
                            }
                        }
                    ]
                }
            }

            version_upsert = WorkflowVersionUpsert._load(version_dict)

            result = loader._topological_sort_workflow_versions(WorkflowVersionUpsertList([version_upsert]))

            assert len(result) == 1
            assert result[0].workflow_external_id == "simple_workflow"

    def test_topological_sort_with_dependencies(self) -> None:
        """Test sorting workflow versions with dependencies."""
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader(client, None, None)

            # Create base workflow version (dependency)
            base_version_dict = {
                "workflowExternalId": "base_workflow",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "simple_task",
                            "type": "function",
                            "parameters": {
                                "function": {
                                    "externalId": "some_function"
                                }
                            }
                        }
                    ]
                }
            }

            # Create dependent workflow version
            dependent_version_dict = {
                "workflowExternalId": "dependent_workflow",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "call_base_workflow",
                            "type": "subworkflow",
                            "parameters": {
                                "subworkflow": {
                                    "workflowExternalId": "base_workflow",
                                    "version": "v1"
                                }
                            }
                        }
                    ]
                }
            }

            base_upsert = WorkflowVersionUpsert._load(base_version_dict)
            dependent_upsert = WorkflowVersionUpsert._load(dependent_version_dict)

            # Sort in wrong order (dependent first, then base)
            result = loader._topological_sort_workflow_versions(
                WorkflowVersionUpsertList([dependent_upsert, base_upsert])
            )

            # Should be sorted correctly: base first, then dependent
            assert len(result) == 2
            assert result[0].workflow_external_id == "base_workflow"  # dependency created first
            assert result[1].workflow_external_id == "dependent_workflow"  # dependent created second

    def test_topological_sort_circular_dependency(self) -> None:
        """Test that circular dependencies are detected."""
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader(client, None, None)

            # Create circular dependency: A depends on B, B depends on A
            version_a_dict = {
                "workflowExternalId": "workflow_a",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "call_workflow_b",
                            "type": "subworkflow",
                            "parameters": {
                                "subworkflow": {
                                    "workflowExternalId": "workflow_b",
                                    "version": "v1"
                                }
                            }
                        }
                    ]
                }
            }

            version_b_dict = {
                "workflowExternalId": "workflow_b",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "call_workflow_a",
                            "type": "subworkflow",
                            "parameters": {
                                "subworkflow": {
                                    "workflowExternalId": "workflow_a",
                                    "version": "v1"
                                }
                            }
                        }
                    ]
                }
            }

            version_a_upsert = WorkflowVersionUpsert._load(version_a_dict)
            version_b_upsert = WorkflowVersionUpsert._load(version_b_dict)

            # Should raise ValueError for circular dependency
            with pytest.raises(ValueError, match="Circular dependency detected"):
                loader._topological_sort_workflow_versions(
                    WorkflowVersionUpsertList([version_a_upsert, version_b_upsert])
                )

    def test_dependency_extraction(self) -> None:
        """Test that dependencies are correctly extracted from subworkflow tasks."""
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader(client, None, None)
            
            # Create workflow version with subworkflow dependency
            version_dict = {
                "workflowExternalId": "dependent_workflow",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "call_external_workflow",
                            "type": "subworkflow",
                            "parameters": {
                                "subworkflow": {
                                    "workflowExternalId": "external_workflow",
                                    "version": "v2"
                                }
                            }
                        }
                    ]
                }
            }
            
            version_upsert = WorkflowVersionUpsert._load(version_dict)
            dependencies = loader._extract_workflow_dependencies(version_upsert)
            
            assert len(dependencies) == 1
            dep = next(iter(dependencies))
            assert dep.workflow_external_id == "external_workflow"
            assert dep.version == "v2"
