"""Tests for instanceSpaces migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node, NodeApply, NodeList

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from cognite_toolkit._cdf_tk.data_classes import ResourceDeployResult
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestInstanceSpacesMigration:
    def test_instance_spaces_with_both_spaces(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that instanceSpaces is populated when both sourceDataInstanceSpace and appDataInstanceSpace exist."""
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
        assert location_filter.instance_spaces is not None
        assert len(location_filter.instance_spaces) == 2
        assert "source_space" in location_filter.instance_spaces
        assert "app_space" in location_filter.instance_spaces

    def test_instance_spaces_with_only_source_space(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that instanceSpaces only contains sourceDataInstanceSpace when appDataInstanceSpace is missing."""
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
                                        "sourceDataInstanceSpace": "source_space",
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

        created_location_filters = toolkit_client_approval.created_resources.get("LocationFilter", [])
        assert len(created_location_filters) == 1
        location_filter = created_location_filters[0]
        assert location_filter.instance_spaces is not None
        assert len(location_filter.instance_spaces) == 1
        assert "source_space" in location_filter.instance_spaces

    def test_instance_spaces_with_only_app_space(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that instanceSpaces only contains appDataInstanceSpace when sourceDataInstanceSpace is missing."""
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
                                        # No sourceDataInstanceSpace
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
        assert location_filter.instance_spaces is not None
        assert len(location_filter.instance_spaces) == 1
        assert "app_space" in location_filter.instance_spaces

    def test_instance_spaces_with_no_spaces(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that instanceSpaces is not included when neither sourceDataInstanceSpace nor appDataInstanceSpace exist."""
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
                                        # No sourceDataInstanceSpace or appDataInstanceSpace
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
        # instanceSpaces should be None (not included) when both spaces are missing
        assert location_filter.instance_spaces is None

    def test_instance_spaces_with_empty_strings(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that empty strings for instance spaces are not included in instanceSpaces."""
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
                                        "sourceDataInstanceSpace": "",  # Empty string
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
        # Empty string should be falsy and not included
        assert location_filter.instance_spaces is not None
        assert len(location_filter.instance_spaces) == 1
        assert "app_space" in location_filter.instance_spaces
        assert "" not in location_filter.instance_spaces

