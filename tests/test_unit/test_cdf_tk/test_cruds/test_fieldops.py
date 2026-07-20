from unittest.mock import MagicMock

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NameId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigRequest,
    FeatureConfiguration,
    ResourceFilters,
    RootLocationConfiguration,
    RootLocationDataFilters,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.infield import DataStorage, InFieldCDMLocationConfigRequest
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    AssetIO,
    DataSetsIO,
    GroupResourceScopedCRUD,
    InFieldCDMLocationConfigIO,
    InfieldV1IO,
    SpaceCRUD,
    ViewIO,
)
from cognite_toolkit._cdf_tk.yaml_classes import InFieldCDMLocationConfigYAML


class TestInfieldV1Loader:
    @pytest.mark.skipif(not Flags.INFIELD.is_enabled(), reason="Alpha feature is not enabled")
    def test_dependent_items(self) -> None:
        item = APMConfigRequest(
            external_id="my_config",
            app_data_space_id="my_app_data_space",
            customer_data_space_id="my_customer_data_space",
            feature_configuration=FeatureConfiguration(
                root_location_configurations=[
                    RootLocationConfiguration(
                        asset_external_id="my_root_asset",
                        template_admins=["my_admin_group1", "my_admin_group2"],
                        checklist_admins=["my_admin_group3"],
                        source_data_instance_space="my_source_data_space",
                        data_filters=RootLocationDataFilters(
                            assets=ResourceFilters(
                                asset_subtree_external_ids=["my_asset_subtree"],
                            ),
                        ),
                    )
                ]
            ),
        )
        dumped = item.dump(context="toolkit")
        dumped["featureConfiguration"]["rootLocationConfigurations"][0]["dataSetExternalId"] = "my_dataset"
        dumped["featureConfiguration"]["rootLocationConfigurations"][0]["dataFilters"]["assets"][
            "dataSetExternalIds"
        ] = ["my_other_dataset"]

        actual = {
            (loader_cls.__name__, identifier) for loader_cls, identifier in InfieldV1IO.get_dependent_items(dumped)
        }

        assert actual == {
            (AssetIO.__name__, ExternalId(external_id="my_root_asset")),
            (DataSetsIO.__name__, ExternalId(external_id="my_dataset")),
            (SpaceCRUD.__name__, SpaceId(space="my_app_data_space")),
            (SpaceCRUD.__name__, SpaceId(space="my_customer_data_space")),
            (SpaceCRUD.__name__, SpaceId(space="my_source_data_space")),
            (GroupResourceScopedCRUD.__name__, NameId(name="my_admin_group1")),
            (GroupResourceScopedCRUD.__name__, NameId(name="my_admin_group2")),
            (GroupResourceScopedCRUD.__name__, NameId(name="my_admin_group3")),
            (DataSetsIO.__name__, ExternalId(external_id="my_other_dataset")),
            (AssetIO.__name__, ExternalId(external_id="my_asset_subtree")),
            (SpaceCRUD.__name__, SpaceId(space="my_source_data_space")),
        }


class TestInFieldCDMLocationConfigCRUD:
    def test_get_dependencies_includes_data_exploration_card_views(self) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(
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
            }
        )
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependencies(config)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ActivitiesCard", version="v2")),
            (
                ViewIO.__name__,
                ViewId(space="customer_idm_extention", external_id="NotificationsCard", version="v2"),
            ),
        }

    def test_get_dependencies_includes_observation_view(self) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(
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
            }
        )
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependencies(config)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ObservationView", version="v2")),
        }

    def test_get_dependencies_includes_observation_view_and_form_view(self) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(
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
                            "formView": {
                                "space": "customer_idm_extention",
                                "version": "v2",
                                "externalId": "ObservationFormView",
                            },
                        },
                    ],
                },
            }
        )
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependencies(config)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ObservationView", version="v2")),
            (
                ViewIO.__name__,
                ViewId(space="customer_idm_extention", external_id="ObservationFormView", version="v2"),
            ),
        }

    def test_get_dependencies_includes_card_views_and_observation_view(self) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(
            {
                "space": "sp_instance",
                "externalId": "my_location_config",
                "dataExplorationConfig": {
                    "assetActivitiesCardView": {
                        "space": "customer_idm_extention",
                        "version": "v2",
                        "externalId": "ActivitiesCard",
                    },
                },
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
            }
        )
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependencies(config)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ActivitiesCard", version="v2")),
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ObservationView", version="v2")),
        }

    def test_get_dependencies_without_data_exploration_config(self) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(
            {"space": "sp_instance", "externalId": "my_location_config"}
        )
        assert list(InFieldCDMLocationConfigIO.get_dependencies(config)) == []

    def test_get_dependencies_ignores_asset_properties_card(self) -> None:
        config = InFieldCDMLocationConfigYAML.model_validate(
            {
                "space": "sp_instance",
                "externalId": "my_location_config",
                "dataExplorationConfig": {
                    "assetPropertiesCardView": {
                        "space": "customer_idm_extention",
                        "version": "v2",
                        "externalId": "PropertiesCard",
                    },
                },
            }
        )
        assert list(InFieldCDMLocationConfigIO.get_dependencies(config)) == []

    def test_get_dependent_items_includes_data_exploration_view_mappings(self) -> None:
        item = {
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
        }
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependent_items(item)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ActivitiesCard", version="v2")),
            (
                ViewIO.__name__,
                ViewId(space="customer_idm_extention", external_id="NotificationsCard", version="v2"),
            ),
        }

    def test_get_dependent_items_includes_observation_view(self) -> None:
        item = {
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
        }
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependent_items(item)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ObservationView", version="v2")),
        }

    def test_get_dependent_items_includes_observation_view_and_form_view(self) -> None:
        item = {
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
                        "formView": {
                            "space": "customer_idm_extention",
                            "version": "v2",
                            "externalId": "ObservationFormView",
                        },
                    },
                ],
            },
        }
        actual = {
            (loader_cls.__name__, identifier)
            for loader_cls, identifier in InFieldCDMLocationConfigIO.get_dependent_items(item)
        }
        assert actual == {
            (ViewIO.__name__, ViewId(space="customer_idm_extention", external_id="ObservationView", version="v2")),
            (
                ViewIO.__name__,
                ViewId(space="customer_idm_extention", external_id="ObservationFormView", version="v2"),
            ),
        }

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
