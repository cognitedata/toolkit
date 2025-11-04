"""Tests for DataExplorationConfig migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataExplorationConfigMigration:
    def test_data_exploration_config_created(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that DataExplorationConfig node is created when FeatureConfiguration has data."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "default-config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "externalId": "loc1",
                                        "assetExternalId": "asset_123",
                                    }
                                ],
                                "observations": {"enabled": True},
                                "activities": {"overviewCard": {}},
                                "documents": {"title": "Documents", "type": "documents", "description": "Document config"},
                                "notifications": {"overviewCard": {}},
                                "assetPageConfiguration": {"propertyCard": {}},
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        # Check that DataExplorationConfig node was created
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        data_exploration_nodes = [
            n for n in created_nodes if n.external_id == "data_exploration_default-config"
        ]
        assert len(data_exploration_nodes) == 1

        data_exploration_node = data_exploration_nodes[0]
        props = data_exploration_node.sources[0].properties

        assert "observations" in props
        assert "activities" in props
        assert "documents" in props
        assert "notifications" in props
        assert "assets" in props

    def test_documents_metadata_prefix_removed(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that metadata. prefix is removed from documents type and description."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "default-config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "externalId": "loc1",
                                        "assetExternalId": "asset_123",
                                    }
                                ],
                                "documents": {
                                    "title": "Documents",
                                    "type": "metadata.documents",
                                    "description": "metadata.document_description",
                                },
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        # Check that DataExplorationConfig node has documents with metadata. prefix removed
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        data_exploration_nodes = [
            n for n in created_nodes if n.external_id == "data_exploration_default-config"
        ]
        assert len(data_exploration_nodes) == 1

        data_exploration_node = data_exploration_nodes[0]
        props = data_exploration_node.sources[0].properties

        assert "documents" in props
        documents = props["documents"]
        assert documents["type"] == "documents"  # metadata. prefix removed
        assert documents["description"] == "document_description"  # metadata. prefix removed
        assert documents["title"] == "Documents"  # unchanged

    def test_documents_without_metadata_prefix(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that documents without metadata. prefix are unchanged."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "default-config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "externalId": "loc1",
                                        "assetExternalId": "asset_123",
                                    }
                                ],
                                "documents": {
                                    "title": "Documents",
                                    "type": "documents",
                                    "description": "document_description",
                                },
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        # Check that DataExplorationConfig node has documents unchanged
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        data_exploration_nodes = [
            n for n in created_nodes if n.external_id == "data_exploration_default-config"
        ]
        assert len(data_exploration_nodes) == 1

        data_exploration_node = data_exploration_nodes[0]
        props = data_exploration_node.sources[0].properties

        assert "documents" in props
        documents = props["documents"]
        assert documents["type"] == "documents"
        assert documents["description"] == "document_description"
        assert documents["title"] == "Documents"

    def test_data_exploration_config_linked_to_all_locations(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that DataExplorationConfig is linked to all InFieldLocationConfig nodes."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "default-config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {"externalId": "loc1", "assetExternalId": "asset_1"},
                                    {"externalId": "loc2", "assetExternalId": "asset_2"},
                                ],
                                "documents": {"title": "Documents", "type": "documents"},
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        # Check that all InFieldLocationConfig nodes have dataExplorationConfig reference
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        location_config_nodes = [
            n for n in created_nodes if n.external_id in ["loc1", "loc2"]
        ]
        assert len(location_config_nodes) == 2

        for location_node in location_config_nodes:
            props = location_node.sources[0].properties
            assert "dataExplorationConfig" in props
            data_exploration_ref = props["dataExplorationConfig"]
            assert data_exploration_ref.space == "APM_Config"
            assert data_exploration_ref.external_id == "data_exploration_default-config"

