"""Tests for dataModels migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from cognite_toolkit._cdf_tk.commands._migrate.infield_config.constants import DEFAULT_LOCATION_FILTER_DATA_MODEL
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataModelsMigration:
    def test_data_models_always_present(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataModels is always present with the default data model."""
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

        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 1
        location_filter = created_location_filters[0]

        # Check that dataModels is always present
        assert location_filter.data_models is not None
        assert len(location_filter.data_models) == 1

        # Check that it's the default data model
        data_model = location_filter.data_models[0]
        assert data_model.space == DEFAULT_LOCATION_FILTER_DATA_MODEL.space
        assert data_model.external_id == DEFAULT_LOCATION_FILTER_DATA_MODEL.external_id
        assert data_model.version == DEFAULT_LOCATION_FILTER_DATA_MODEL.version

    def test_data_models_with_other_fields(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that dataModels is present even when other fields are also migrated."""
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

        MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InfieldV2ConfigCreator(toolkit_client_approval.client),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 1
        location_filter = created_location_filters[0]

        # Check that both instanceSpaces and dataModels are present
        assert location_filter.instance_spaces is not None
        assert location_filter.data_models is not None
        assert len(location_filter.data_models) == 1

        # Verify it's the default data model
        data_model = location_filter.data_models[0]
        assert data_model.space == "infield_cdm_source_desc_sche_asset_file_ts"
        assert data_model.external_id == "InFieldOnCDM"
        assert data_model.version == "v1"

    def test_data_models_multiple_locations(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that all locations get the same default data model."""
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
                                    {"externalId": "loc1"},
                                    {"externalId": "loc2"},
                                    {"externalId": "loc3"},
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

        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 3

        # Check that all location filters have the same default data model
        for location_filter in created_location_filters:
            assert location_filter.data_models is not None
            assert len(location_filter.data_models) == 1
            data_model = location_filter.data_models[0]
            assert data_model.space == "infield_cdm_source_desc_sche_asset_file_ts"
            assert data_model.external_id == "InFieldOnCDM"
            assert data_model.version == "v1"

