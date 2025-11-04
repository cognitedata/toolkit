"""Tests for disciplines migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDisciplinesMigration:
    def test_disciplines_with_disciplines(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that disciplines are migrated when present in FeatureConfiguration."""
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
                                "disciplines": [
                                    {"externalId": "mechanical", "name": "Mechanical"},
                                    {"externalId": "electrical", "name": "Electrical"},
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

        # Check that disciplines are present
        assert "disciplines" in location_props
        disciplines = location_props["disciplines"]
        assert len(disciplines) == 2
        assert disciplines[0]["externalId"] == "mechanical"
        assert disciplines[0]["name"] == "Mechanical"
        assert disciplines[1]["externalId"] == "electrical"
        assert disciplines[1]["name"] == "Electrical"

    def test_disciplines_with_multiple_locations(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that disciplines are shared across all locations."""
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
                                        "assetExternalId": "asset_1",
                                    },
                                    {
                                        "externalId": "loc2",
                                        "assetExternalId": "asset_2",
                                    },
                                ],
                                "disciplines": [
                                    {"externalId": "mechanical", "name": "Mechanical"},
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
        assert len(created_nodes) == 2

        # Both locations should have the same disciplines
        for location_node in created_nodes:
            location_props = location_node.sources[0].properties
            assert "disciplines" in location_props
            disciplines = location_props["disciplines"]
            assert len(disciplines) == 1
            assert disciplines[0]["externalId"] == "mechanical"
            assert disciplines[0]["name"] == "Mechanical"

    def test_disciplines_with_no_disciplines(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that disciplines are not included when not present in FeatureConfiguration."""
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
                                # No disciplines
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

        # Check that disciplines is not present
        assert "disciplines" not in location_props

    def test_disciplines_with_empty_list(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that disciplines are not included when disciplines is an empty list."""
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
                                "disciplines": [],  # Empty list
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

        # Check that disciplines is not present when empty
        assert "disciplines" not in location_props

