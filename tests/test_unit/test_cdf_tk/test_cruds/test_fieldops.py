from unittest.mock import MagicMock

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigRequest,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.infield import DataStorage, InFieldCDMLocationConfigRequest
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.resource_ios import (
    InFieldCDMLocationConfigIO,
    SpaceCRUD,
    ViewIO,
)
from cognite_toolkit._cdf_tk.yaml_classes import InFieldCDMLocationConfigYAML


class TestInFieldCDMLocationConfigCRUD:
    @pytest.mark.parametrize(
        "raw_config, expected",
        [
            pytest.param(
                {
                    "space": "sp_instance",
                    "externalId": "my_location_config",
                    "dataExplorationConfig": {
                        "assetActivitiesCardView": {
                            "space": "customer_idm_extention",
                            "version": "v2",
                            "externalId": "ActivitiesCard",
                        },
                        "assetNotificationsCardView": {
                            "space": "customer_idm_extention",
                            "version": "v2",
                            "externalId": "NotificationsCard",
                        },
                    },
                },
                {
                    (SpaceCRUD.__name__, SpaceId(space="sp_instance")),
                    (
                        ViewIO.__name__,
                        ViewId(space="customer_idm_extention", external_id="ActivitiesCard", version="v2"),
                    ),
                    (
                        ViewIO.__name__,
                        ViewId(space="customer_idm_extention", external_id="NotificationsCard", version="v2"),
                    ),
                },
                id="data-exploration-card-views",
            ),
            pytest.param(
                {
                    "space": "sp_instance",
                    "externalId": "my_location_config",
                    "viewMappings": {
                        "observation": [
                            {
                                "view": {
                                    "space": "customer_idm_extention",
                                    "version": "v2",
                                    "externalId": "ObservationView",
                                },
                            },
                        ],
                    },
                },
                {
                    (SpaceCRUD.__name__, SpaceId(space="sp_instance")),
                    (
                        ViewIO.__name__,
                        ViewId(space="customer_idm_extention", external_id="ObservationView", version="v2"),
                    ),
                },
                id="observation-view",
            ),
            pytest.param(
                {
                    "space": "sp_instance",
                    "externalId": "my_location_config",
                    "dataExplorationConfig": {
                        "assetPropertiesCardConfig": {
                            "name": {
                                "displayName": "Asset name",
                                "orderNumber": 0,
                            },
                        },
                    },
                },
                {(SpaceCRUD.__name__, SpaceId(space="sp_instance"))},
                id="asset-properties-card-config-is-not-a-view",
            ),
            pytest.param(
                {"space": "sp_instance", "externalId": "my_location_config"},
                {(SpaceCRUD.__name__, SpaceId(space="sp_instance"))},
                id="no-data-exploration-config",
            ),
            pytest.param(
                {
                    "space": "sp_instance",
                    "externalId": "my_location_config",
                    "viewMappings": {
                        "asset": {"space": "cdf_cdm", "externalId": "CogniteAsset", "version": "v1"},
                        "operation": {"space": "cdf_idm", "externalId": "CogniteOperation", "version": "v1"},
                    },
                },
                {
                    (SpaceCRUD.__name__, SpaceId(space="sp_instance")),
                    (ViewIO.__name__, ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")),
                    (ViewIO.__name__, ViewId(space="cdf_idm", external_id="CogniteOperation", version="v1")),
                },
                id="direct-view-mappings",
            ),
            pytest.param(
                {
                    "space": "sp_instance",
                    "externalId": "my_location_config",
                    "dataFilters": {
                        "assets": {"instanceSpaces": ["migrated_assets"]},
                        "maintenanceOrders": {"instanceSpaces": ["APM_SourceData_3_LOR_NORWAY_cdm"]},
                    },
                    "dataStorage": {
                        "rootLocation": {"space": "migrated_assets", "externalId": "wefwef"},
                        "appInstanceSpace": "app_data_instance_space_LOR_NORWAY_cdm",
                    },
                },
                {
                    (SpaceCRUD.__name__, SpaceId(space="sp_instance")),
                    (SpaceCRUD.__name__, SpaceId(space="migrated_assets")),
                    (SpaceCRUD.__name__, SpaceId(space="APM_SourceData_3_LOR_NORWAY_cdm")),
                    (SpaceCRUD.__name__, SpaceId(space="app_data_instance_space_LOR_NORWAY_cdm")),
                },
                id="data-filters-and-data-storage-spaces",
            ),
        ],
    )
    def test_get_dependencies(self, raw_config: dict, expected: set) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(raw_config)
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependencies(config)
        }
        assert actual == expected

    def test_skip_illegal_configuration(self) -> None:
        legacy_space = "my_infield_legacy_space"
        item = InFieldCDMLocationConfigRequest(
            external_id="my_config",
            space="my_space",
            data_storage=DataStorage(app_instance_space=legacy_space),
        )
        legacy = APMConfigRequest(
            external_id="my_last_config",
            feature_configuration=FeatureConfiguration(
                root_location_configurations=[RootLocationConfiguration(app_data_instance_space=legacy_space)]
            ),
        )
        with monkeypatch_toolkit_client() as client:
            my_console = MagicMock(spec=Console)
            client.infield.apm_config.list.return_value = [legacy]
            io = InFieldCDMLocationConfigIO(client, None, my_console)

            created = io.create([item])

            assert len(created) == 0
            assert my_console.print.called
            message = my_console.print.call_args[0][1]
            assert message.startswith(f"Skipping creation of infield CDM location configs {item.as_id()!s}.")

    def test_cdm_only_project_no_apm_config_view(self) -> None:
        item = InFieldCDMLocationConfigRequest(
            external_id="my_config",
            space="my_space",
            data_storage=DataStorage(app_instance_space="my_space"),
        )
        with monkeypatch_toolkit_client() as client:
            my_console = MagicMock(spec=Console)
            client.infield.apm_config.list.side_effect = ToolkitAPIError(
                "One or more views do not exist: 'APM_Config:APM_Config/1'", code=400
            )
            io = InFieldCDMLocationConfigIO(client, None, my_console)

            io.create([item])

            client.infield.cdm_config.create.assert_called_once_with([item])
            assert not my_console.print.called
