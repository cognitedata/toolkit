from pathlib import Path
from typing import Any

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node, NodeApply, NodeList, View

from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import (
    APMConfig,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from cognite_toolkit._cdf_tk.data_classes import ResourceDeployResult
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestInfieldV2ConfigCreator:
    @pytest.mark.parametrize("dry_run", [pytest.param(True, id="dry_run"), pytest.param(False, id="not_dry_run")])
    def test_create_infield_v2_configs_basic(
        self, dry_run: bool, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test basic migration of APMConfig to InField V2 format."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        # Create a mock APMConfig node
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
                            "customerDataSpaceId": "APM_SourceData",
                            "customerDataSpaceVersion": "1",
                            "name": "Test Config",
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "externalId": "location_1",
                                        "assetExternalId": "asset_1",
                                        "displayName": "Location 1",
                                        "appDataInstanceSpace": "app_space_1",
                                        "sourceDataInstanceSpace": "source_space_1",
                                        "dataSetId": 123,
                                        "featureToggles": {
                                            "threeD": True,
                                            "documents": True,
                                        },
                                        "templateAdmins": ["admin1"],
                                        "checklistAdmins": ["admin2"],
                                    }
                                ],
                            },
                        }
                    }
                },
            }
        )

        # Add the APMConfig node to the approval client so it can be retrieved
        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=dry_run,
            verbose=False,
            output_dir=tmp_path,
        )

        # LocationFilters are now deployed separately via Location Filters API
        assert "location filters" in results
        location_filter_result = results["location filters"]
        assert isinstance(location_filter_result, ResourceDeployResult)
        assert location_filter_result.created == 1

        # InFieldLocationConfig nodes are deployed via Data Modeling Instance API
        assert "nodes" in results
        node_result = results["nodes"]
        assert isinstance(node_result, ResourceDeployResult)
        assert node_result.created == 1

        # Check that config files were created (both LocationFilter and Node files)
        location_filter_configs = list(tmp_path.rglob("*LocationFilter.yaml"))
        node_configs = list(tmp_path.rglob("*Node.yaml"))
        assert len(location_filter_configs) == 1
        assert len(node_configs) == 1

        if not dry_run:
            # Check LocationFilter resource (deployed via Location Filters API)
            created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
            assert len(created_location_filters) == 1
            location_filter = created_location_filters[0]
            from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilterWrite
            assert isinstance(location_filter, LocationFilterWrite)
            assert location_filter.external_id == "location_filter_location_1"
            assert location_filter.name == "Location 1"
            assert location_filter.description == "InField location, migrated from old location configuration"
            # Check that instanceSpaces is populated with both sourceDataInstanceSpace and appDataInstanceSpace
            # Note: Detailed instanceSpaces tests are in test_infield_instance_spaces.py
            assert location_filter.instance_spaces is not None
            assert len(location_filter.instance_spaces) == 2
            assert "source_space_1" in location_filter.instance_spaces
            assert "app_space_1" in location_filter.instance_spaces

            # Check InFieldLocationConfig node (deployed via Data Modeling Instance API)
            created_nodes = toolkit_client_approval.created_resources.get("Node", [])
            assert len(created_nodes) == 1
            location_node = created_nodes[0]
            assert isinstance(location_node, NodeApply)
            assert location_node.external_id == "location_1"
            assert location_node.space == "APM_Config"
            assert len(location_node.sources) == 1
            assert location_node.sources[0].source.external_id == "InFieldLocationConfig"

            # Check properties - should have rootLocationExternalId referencing the location filter
            location_props = location_node.sources[0].properties
            assert location_props["rootLocationExternalId"] == "location_filter_location_1"

    def test_create_infield_v2_configs_multiple_locations(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test migration with multiple root locations."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "multi_location_config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "customerDataSpaceId": "APM_SourceData",
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "externalId": "loc1",
                                        "assetExternalId": "asset1",
                                    },
                                    {
                                        "externalId": "loc2",
                                        "assetExternalId": "asset2",
                                    },
                                ],
                            },
                        }
                    }
                },
            }
        )

        # Add the APMConfig node to the approval client so it can be retrieved
        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        # LocationFilters are now deployed separately via Location Filters API
        assert "location filters" in results
        location_filter_result = results["location filters"]
        assert location_filter_result.created == 2

        # InFieldLocationConfig nodes are deployed via Data Modeling Instance API
        assert "nodes" in results
        node_result = results["nodes"]
        assert node_result.created == 2

        # Check LocationFilters (they are now LocationFilter resources, not Node resources)
        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 2
        location_filter_ids = [lf.external_id for lf in created_location_filters]
        assert "location_filter_loc1" in location_filter_ids
        assert "location_filter_loc2" in location_filter_ids

        # Check InFieldLocationConfig nodes
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 2

        # Should have two location config nodes
        location_nodes = [n for n in created_nodes if n.external_id in ["loc1", "loc2"]]
        assert len(location_nodes) == 2

        # Verify location configs reference their location filters
        loc1_node = next((n for n in location_nodes if n.external_id == "loc1"), None)
        loc2_node = next((n for n in location_nodes if n.external_id == "loc2"), None)
        assert loc1_node is not None
        assert loc2_node is not None
        assert loc1_node.sources[0].properties["rootLocationExternalId"] == "location_filter_loc1"
        assert loc2_node.sources[0].properties["rootLocationExternalId"] == "location_filter_loc2"

    def test_create_infield_v2_configs_empty(self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path) -> None:
        """Test migration with no root locations."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "empty_config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "featureConfiguration": {
                                "rootLocationConfigurations": [],
                            },
                        }
                    }
                },
            }
        )

        # Add the APMConfig node to the approval client so it can be retrieved
        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        assert "nodes" in results
        result = results["nodes"]
        # No locations, so no nodes created
        assert result.created == 0

    def test_create_infield_v2_configs_no_feature_config(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test migration with missing feature configuration."""
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)

        apm_config_node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "no_feature_config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {},
                    }
                },
            }
        )

        # Add the APMConfig node to the approval client so it can be retrieved
        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        assert "nodes" in results
        result = results["nodes"]
        assert result.created == 0

    def test_external_id_generation_with_external_id(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test external ID generation when externalId exists in old config."""
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
                                        "externalId": "my_location",
                                        "assetExternalId": "my_asset",
                                    }
                                ],
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        created_nodes = toolkit_client_approval.created_resources["Node"]
        location_node = next((n for n in created_nodes if n.external_id == "my_location"), None)
        assert location_node is not None
        # Should use externalId directly when it exists
        assert location_node.external_id == "my_location"

    def test_external_id_generation_with_only_asset_external_id(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test external ID generation when only assetExternalId exists (should add index postfix)."""
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
                                        # No externalId, only assetExternalId
                                        "assetExternalId": "shared_asset",
                                    },
                                    {
                                        # No externalId, only assetExternalId (same asset)
                                        "assetExternalId": "shared_asset",
                                    },
                                ],
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        created_nodes = toolkit_client_approval.created_resources["Node"]
        # Should have 2 location config nodes with index postfix for uniqueness
        location_nodes = [n for n in created_nodes if n.external_id.startswith("shared_asset_")]
        assert len(location_nodes) == 2
        # Should have index 0 and 1
        external_ids = [n.external_id for n in location_nodes]
        assert "shared_asset_0" in external_ids
        assert "shared_asset_1" in external_ids

    def test_external_id_generation_with_neither_external_id(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test external ID generation when neither externalId nor assetExternalId exists (should generate UUID)."""
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
                                        # No externalId, no assetExternalId
                                    }
                                ],
                            },
                        }
                    }
                },
            }
        )

        toolkit_client_approval.append(Node, apm_config_node)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        created_nodes = toolkit_client_approval.created_resources["Node"]
        # Should have location config with generated UUID
        location_nodes = [n for n in created_nodes if n.external_id.startswith("infield_location_")]
        assert len(location_nodes) == 1
        # Should start with infield_location_ prefix
        assert location_nodes[0].external_id.startswith("infield_location_")


