"""Tests for featureToggles migration in InField V2 config migration."""

from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModel, Node

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InfieldV2ConfigCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestFeatureTogglesMigration:
    def test_feature_toggles_with_all_fields(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that featureToggles is migrated when all fields are present."""
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
                                        "featureToggles": {
                                            "threeD": True,
                                            "trends": False,
                                            "documents": True,
                                            "workorders": False,
                                            "notifications": True,
                                            "media": False,
                                            "templateChecklistFlow": True,
                                            "workorderChecklistFlow": False,
                                            "observations": {
                                                "isEnabled": True,
                                                "isWriteBackEnabled": False,
                                                "notificationsEndpointExternalId": "notif_endpoint",
                                                "attachmentsEndpointExternalId": "attach_endpoint",
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

        # Check that featureToggles is present
        assert "featureToggles" in location_props
        feature_toggles = location_props["featureToggles"]

        # Check all boolean fields
        assert feature_toggles["threeD"] is True
        assert feature_toggles["trends"] is False
        assert feature_toggles["documents"] is True
        assert feature_toggles["workorders"] is False
        assert feature_toggles["notifications"] is True
        assert feature_toggles["media"] is False
        assert feature_toggles["templateChecklistFlow"] is True
        assert feature_toggles["workorderChecklistFlow"] is False

        # Check observations nested object
        assert "observations" in feature_toggles
        observations = feature_toggles["observations"]
        assert observations["isEnabled"] is True
        assert observations["isWriteBackEnabled"] is False
        assert observations["notificationsEndpointExternalId"] == "notif_endpoint"
        assert observations["attachmentsEndpointExternalId"] == "attach_endpoint"

    def test_feature_toggles_with_partial_fields(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that featureToggles is migrated when only some fields are present."""
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
                                        "featureToggles": {
                                            "threeD": True,
                                            "documents": False,
                                            # Other fields missing - should still work
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

        # Check that featureToggles is present
        assert "featureToggles" in location_props
        feature_toggles = location_props["featureToggles"]

        # Check that only the present fields are migrated
        assert feature_toggles["threeD"] is True
        assert feature_toggles["documents"] is False

    def test_feature_toggles_with_no_feature_toggles(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that featureToggles is not included when missing from old config."""
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
                                        # No featureToggles
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

        # Check that featureToggles is not present when missing from old config
        assert "featureToggles" not in location_props

    def test_feature_toggles_with_empty_observations(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        """Test that featureToggles works when observations is present but empty."""
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
                                        "featureToggles": {
                                            "threeD": True,
                                            "observations": {},  # Empty observations
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

        # Check that featureToggles is present
        assert "featureToggles" in location_props
        feature_toggles = location_props["featureToggles"]
        assert feature_toggles["threeD"] is True
        assert "observations" in feature_toggles
        assert feature_toggles["observations"] == {}

