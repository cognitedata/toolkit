"""Integration tests for InField V2 config migration.

This test suite verifies that all components of the migration work together correctly.
Individual field migrations are tested in their respective test files:
- test_infield_v2_config_creator.py: Core migration logic and external ID generation
- test_infield_instance_spaces.py: instanceSpaces field migration

This file focuses on end-to-end integration scenarios.
"""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from cognite_toolkit._cdf_tk.data_classes import ResourceDeployResult
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestInfieldV2ConfigIntegration:
    """Integration tests for complete InField V2 config migration."""

    def test_complete_migration_with_all_fields(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test complete migration with all currently supported fields."""
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
                            "customerDataSpaceId": "APM_SourceData",
                            "customerDataSpaceVersion": "1",
                            "name": "Complete Config",
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "externalId": "complete_location",
                                        "assetExternalId": "complete_asset",
                                        "displayName": "Complete Location",
                                        "sourceDataInstanceSpace": "source_space",
                                        "appDataInstanceSpace": "app_space",
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

        # Verify both resource types were created
        assert "location filters" in results
        assert "nodes" in results
        location_filter_result = results["location filters"]
        node_result = results["nodes"]

        assert location_filter_result.created == 1
        assert node_result.created == 1

        # Verify LocationFilter was created with all migrated fields
        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 1
        location_filter = created_location_filters[0]
        assert location_filter.external_id == "location_filter_complete_location"
        assert location_filter.name == "Complete Location"
        assert location_filter.instance_spaces is not None
        assert len(location_filter.instance_spaces) == 2

        # Verify InFieldLocationConfig node was created
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 1
        location_node = created_nodes[0]
        assert location_node.external_id == "complete_location"
        assert location_node.sources[0].properties["rootLocationExternalId"] == "location_filter_complete_location"

    def test_migration_with_multiple_locations_complete(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test migration of multiple locations with all supported fields."""
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
                                        "sourceDataInstanceSpace": "source1",
                                        "appDataInstanceSpace": "app1",
                                    },
                                    {
                                        "externalId": "loc2",
                                        "sourceDataInstanceSpace": "source2",
                                        # Missing appDataInstanceSpace - should still work
                                    },
                                    {
                                        # Missing externalId - should use assetExternalId with index
                                        "assetExternalId": "shared_asset",
                                        "sourceDataInstanceSpace": "source3",
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

        # Should create 3 LocationFilters and 3 InFieldLocationConfig nodes
        assert results["location filters"].created == 3
        assert results["nodes"].created == 3

        # Verify all LocationFilters have correct instanceSpaces
        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 3

        location_filter_by_id = {lf.external_id: lf for lf in created_location_filters}
        
        # loc1 should have both spaces
        assert location_filter_by_id["location_filter_loc1"].instance_spaces == ["source1", "app1"]
        
        # loc2 should only have source space
        assert location_filter_by_id["location_filter_loc2"].instance_spaces == ["source2"]
        
        # shared_asset should have source3 (LocationFilter external ID doesn't use index)
        assert location_filter_by_id["location_filter_shared_asset"].instance_spaces == ["source3"]

        # Verify all InFieldLocationConfig nodes reference their LocationFilters correctly
        created_nodes = toolkit_client_approval.created_resources.get("Node", [])
        assert len(created_nodes) == 3

        node_by_id = {node.external_id: node for node in created_nodes}
        assert node_by_id["loc1"].sources[0].properties["rootLocationExternalId"] == "location_filter_loc1"
        assert node_by_id["loc2"].sources[0].properties["rootLocationExternalId"] == "location_filter_loc2"
        # InFieldLocationConfig uses index for uniqueness (index 2 since it's the third item in the list)
        # but references LocationFilter without index
        assert "shared_asset_2" in node_by_id
        assert node_by_id["shared_asset_2"].sources[0].properties["rootLocationExternalId"] == "location_filter_shared_asset"

