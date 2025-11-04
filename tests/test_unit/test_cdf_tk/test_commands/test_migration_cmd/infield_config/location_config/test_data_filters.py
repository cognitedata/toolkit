"""Tests for dataFilters migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataFiltersMigration:
    def test_data_filters_with_all_sections(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataFilters is migrated when all sections (general, assets, files, timeseries) are present."""
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
                                        "dataFilters": {
                                            "general": {
                                                "datasetIds": [1, 2, 3],
                                                "spaces": ["space1", "space2"],
                                            },
                                            "assets": {
                                                "assetSubtreeExternalIds": ["asset1", "asset2"],
                                                "rootAssetExternalIds": ["root1"],
                                            },
                                            "files": {
                                                "externalIdPrefix": "file_prefix_",
                                                "datasetIds": [4, 5],
                                            },
                                            "timeseries": {
                                                "spaces": ["ts_space1"],
                                                "externalIdPrefix": "ts_prefix_",
                                            },
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

        # Check that dataFilters is present with all sections
        assert "dataFilters" in location_props
        data_filters = location_props["dataFilters"]
        
        assert "general" in data_filters
        assert data_filters["general"]["datasetIds"] == [1, 2, 3]
        assert data_filters["general"]["spaces"] == ["space1", "space2"]
        
        assert "assets" in data_filters
        assert data_filters["assets"]["assetSubtreeExternalIds"] == ["asset1", "asset2"]
        assert data_filters["assets"]["rootAssetExternalIds"] == ["root1"]
        
        assert "files" in data_filters
        assert data_filters["files"]["externalIdPrefix"] == "file_prefix_"
        assert data_filters["files"]["datasetIds"] == [4, 5]
        
        assert "timeseries" in data_filters
        assert data_filters["timeseries"]["spaces"] == ["ts_space1"]
        assert data_filters["timeseries"]["externalIdPrefix"] == "ts_prefix_"

    def test_data_filters_with_partial_sections(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataFilters is migrated when only some sections are present."""
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
                                        "dataFilters": {
                                            "general": {
                                                "datasetIds": [1],
                                            },
                                            "assets": {
                                                "assetSubtreeExternalIds": ["asset1"],
                                            },
                                            # files and timeseries are missing
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

        # Check that dataFilters is present with only the sections that were provided
        assert "dataFilters" in location_props
        data_filters = location_props["dataFilters"]
        
        assert "general" in data_filters
        assert data_filters["general"]["datasetIds"] == [1]
        
        assert "assets" in data_filters
        assert data_filters["assets"]["assetSubtreeExternalIds"] == ["asset1"]
        
        # files and timeseries should not be present
        assert "files" not in data_filters
        assert "timeseries" not in data_filters

    def test_data_filters_with_no_data_filters(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataFilters is not included when dataFilters is missing in old config."""
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
                                        # No dataFilters
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

        # Check that dataFilters is not present
        assert "dataFilters" not in location_props

    def test_data_filters_with_empty_dict(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataFilters is not included when dataFilters is an empty dict."""
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
                                        "dataFilters": {},  # Empty dict
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

        # Empty dict should be treated as falsy and not included
        assert "dataFilters" not in location_props

    def test_data_filters_with_other_fields(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataFilters is migrated correctly when other fields are also present."""
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
                                        "appDataInstanceSpace": "app_space",
                                        "dataFilters": {
                                            "general": {
                                                "datasetIds": [1],
                                            },
                                        },
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

        # Check that both dataFilters and other fields are present
        assert "dataFilters" in location_props
        assert "appInstanceSpace" in location_props
        assert "featureToggles" in location_props
        
        data_filters = location_props["dataFilters"]
        assert "general" in data_filters
        assert data_filters["general"]["datasetIds"] == [1]

