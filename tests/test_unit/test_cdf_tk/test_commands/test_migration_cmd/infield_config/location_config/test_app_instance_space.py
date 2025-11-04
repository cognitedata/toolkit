"""Tests for appInstanceSpace migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestAppInstanceSpaceMigration:
    def test_app_instance_space_present(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that appInstanceSpace is migrated when appDataInstanceSpace exists."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "test_config",
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
                                        "appDataInstanceSpace": "app_space_123",
                                    }
                                ],
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

        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 1
        location_node = created_nodes[0]
        location_props = location_node.sources[0].properties

        # Check that appInstanceSpace is present
        assert "appInstanceSpace" in location_props
        assert location_props["appInstanceSpace"] == "app_space_123"

    def test_app_instance_space_missing(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that appInstanceSpace is not included when appDataInstanceSpace is missing."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "test_config",
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
                                        # No appDataInstanceSpace
                                    }
                                ],
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

        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 1
        location_node = created_nodes[0]
        location_props = location_node.sources[0].properties

        # Check that appInstanceSpace is not present when appDataInstanceSpace is missing
        assert "appInstanceSpace" not in location_props

    def test_app_instance_space_empty_string(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that appInstanceSpace is not included when appDataInstanceSpace is an empty string."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "test_config",
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
                                        "appDataInstanceSpace": "",  # Empty string
                                    }
                                ],
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

        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 1
        location_node = created_nodes[0]
        location_props = location_node.sources[0].properties

        # Check that appInstanceSpace is not present when appDataInstanceSpace is empty string
        assert "appInstanceSpace" not in location_props

    def test_app_instance_space_with_other_fields(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that appInstanceSpace works together with other migrated fields."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "test_config",
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
                                        "appDataInstanceSpace": "app_space_456",
                                        "assetExternalId": "asset_456",
                                        "sourceDataInstanceSpace": "source_space",
                                        "featureToggles": {
                                            "threeD": True,
                                        },
                                    }
                                ],
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

        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 1
        location_node = created_nodes[0]
        location_props = location_node.sources[0].properties

        # Check that appInstanceSpace and other fields are present
        assert "appInstanceSpace" in location_props
        assert location_props["appInstanceSpace"] == "app_space_456"
        assert "rootAsset" in location_props
        assert "featureToggles" in location_props

