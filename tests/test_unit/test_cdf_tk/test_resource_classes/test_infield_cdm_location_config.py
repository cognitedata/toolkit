from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.infield_cdm_location_config import InFieldCDMLocationConfigYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "MyLocationConfig"},
        {"Missing required field: 'externalId'", "Missing required field: 'space'"},
        id="Missing required fields: externalId and space",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "unknownField": "invalid_value",
            "anotherUnknownField": 123,
            "featureToggles": {
                "threeD": True,
                "invalidToggle": "bad_value",
            },
        },
        {
            "In featureToggles unknown field: 'invalidToggle'",
            "Unknown field: 'anotherUnknownField'",
            "Unknown field: 'unknownField'",
        },
        id="Multiple extra fields at different levels",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "featureToggles": {
                "threeD": "not_a_boolean",
                "trends": 123,
            },
            "accessManagement": {
                "templateAdmins": "should_be_a_list",
                "checklistAdmins": 456,
            },
            "dataFilters": {
                "files": {
                    "path": {
                        "space": 123,  # Should be string
                        "externalId": True,  # Should be string
                    },
                    "instanceSpaces": "should_be_list",  # Should be list
                },
            },
            "viewMappings": {
                "asset": {
                    "space": "my_space",
                    "version": "v1",
                    # Missing externalId
                },
            },
            "disciplines": [
                {
                    "name": "Engineering",
                    # Missing externalId
                },
            ],
        },
        {
            "In accessManagement.checklistAdmins input should be a valid list. Got 456.",
            "In accessManagement.templateAdmins input should be a valid list. Got 'should_be_a_list'.",
            "In dataFilters.files.instanceSpaces input should be a valid list. Got 'should_be_list'.",
            "In dataFilters.files.path.externalId input should be a valid string. Got True of type bool. Hint: Use double quotes to force string.",
            "In dataFilters.files.path.space input should be a valid string. Got 123 of type int. Hint: Use double quotes to force string.",
            "In disciplines[1] missing required field: 'externalId'",
            "In featureToggles.threeD input should be a valid boolean. Got 'not_a_boolean' of type str.",
            "In featureToggles.trends input should be a valid boolean. Got 123 of type int.",
            "In viewMappings.asset missing required field: 'externalId'",
        },
        id="Multiple type mismatches across nested structures",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "unknownField": "bad_value",
            },
        },
        {"In dataExplorationConfig unknown field: 'unknownField'"},
        id="Unknown field in dataExplorationConfig",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetPropertiesCardConfig": {
                    "name": {
                        "orderNumber": -1,
                    },
                },
            },
        },
        {
            "In dataExplorationConfig.assetPropertiesCardConfig.name.orderNumber input should be greater than or equal to 0"
        },
        id="Negative orderNumber in dataExplorationConfig.assetPropertiesCardConfig",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetPropertiesCardConfig": {
                    "name": {
                        "unknownField": "bad_value",
                    },
                },
            },
        },
        {"In dataExplorationConfig.assetPropertiesCardConfig.name unknown field: 'unknownField'"},
        id="Unknown field in dataExplorationConfig.assetPropertiesCardConfig entry",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetPropertiesCardConfig": {
                    "invalid@": {},
                },
            },
        },
        {
            "In dataExplorationConfig.assetPropertiesCardConfig property 'invalid@' does not match the required pattern: ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,253}[a-zA-Z0-9]?$"
        },
        id="Invalid property key in dataExplorationConfig.assetPropertiesCardConfig",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [],
            },
        },
        {"In viewMappings.observation list should have at least 1 item after validation, not 0"},
        id="Empty observation list in viewMappings",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsA",
                        },
                    },
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsB",
                        },
                    },
                ],
            },
        },
        {"In viewMappings.observation list should have at most 1 item after validation, not 2"},
        id="Multiple observations in viewMappings not supported",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {"space": "my_space", "version": "v1", "externalId": "ObsA"},
                ],
            },
        },
        {
            "In viewMappings.observation[1] missing required field: 'view'",
            "In viewMappings.observation[1] unknown field: 'externalId'",
            "In viewMappings.observation[1] unknown field: 'space'",
            "In viewMappings.observation[1] unknown field: 'version'",
        },
        id="Flat legacy ViewMapping shape in viewMappings.observation",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "asset": {
                    "space": "my_space",
                    "version": "v1",
                    "externalId": "123invalid",
                },
            },
        },
        {"In viewMappings.asset.externalId string should match pattern '^[a-zA-Z]([a-zA-Z0-9_]{0,253}[a-zA-Z0-9])?$'"},
        id="Invalid externalId pattern in viewMappings.asset",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                        },
                    },
                ],
            },
        },
        {"In viewMappings.observation[1].view missing required field: 'externalId'"},
        id="Missing required field in viewMappings.observation view",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "unknownField": "bad_value",
                    },
                ],
            },
        },
        {"In viewMappings.observation[1] unknown field: 'unknownField'"},
        id="Unknown field in viewMappings.observation",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "writeBack": {
                            "notificationsEndpointExternalId": "notif-endpoint",
                            "unknownField": "bad_value",
                        },
                    },
                ],
            },
        },
        {"In viewMappings.observation[1].writeBack unknown field: 'unknownField'"},
        id="Unknown field in viewMappings.observation.writeBack",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "writeBack": {},
                    },
                ],
            },
        },
        {"In viewMappings.observation[1].writeBack missing required field: 'notificationsEndpointExternalId'"},
        id="Missing notificationsEndpointExternalId in viewMappings.observation.writeBack",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "fieldsConfig": {
                            "assets": {
                                "orderNumber": -1,
                            },
                        },
                    },
                ],
            },
        },
        {"In viewMappings.observation[1].fieldsConfig.assets.orderNumber input should be greater than or equal to 0"},
        id="Negative orderNumber in viewMappings.observation.fieldsConfig",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "fieldsConfig": {
                            "assets": {
                                "unknownField": "bad_value",
                            },
                        },
                    },
                ],
            },
        },
        {"In viewMappings.observation[1].fieldsConfig.assets unknown field: 'unknownField'"},
        id="Unknown field in viewMappings.observation.fieldsConfig entry",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "fieldsConfig": {
                            "invalid@": {},
                        },
                    },
                ],
            },
        },
        {
            "In viewMappings.observation[1].fieldsConfig property 'invalid@' does not match the required pattern: ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,253}[a-zA-Z0-9]?$"
        },
        id="Invalid property key in viewMappings.observation.fieldsConfig",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetActivitiesCardView": {
                    "space": "my_space",
                    "version": "v1",
                    "externalId": "MyActivityView",
                    "unknownNested": "x",
                },
            },
        },
        {"In dataExplorationConfig.assetActivitiesCardView unknown field: 'unknownNested'"},
        id="Unknown field in dataExplorationConfig.assetActivitiesCardView",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetActivitiesCardView": {
                    "space": "my_space",
                    "version": "v1",
                },
            },
        },
        {"In dataExplorationConfig.assetActivitiesCardView missing required field: 'externalId'"},
        id="Missing required field in dataExplorationConfig.assetActivitiesCardView",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetNotificationsCardView": {
                    "space": "my_space",
                    "version": "v1",
                    "externalId": "MyNotificationView",
                    "extraProp": 1,
                },
            },
        },
        {"In dataExplorationConfig.assetNotificationsCardView unknown field: 'extraProp'"},
        id="Unknown field in dataExplorationConfig.assetNotificationsCardView",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetNotificationsCardView": {
                    "space": "my_space",
                    "version": "v1",
                },
            },
        },
        {"In dataExplorationConfig.assetNotificationsCardView missing required field: 'externalId'"},
        id="Missing required field in dataExplorationConfig.assetNotificationsCardView",
    )


class TestInfieldCDMLocationConfigYAML:
    @pytest.mark.parametrize(
        "data",
        list(
            find_resources("InFieldCDMLocationConfig", resource_dir="cdf_applications", base=COMPLETE_ORG_ALPHA_FLAGS)
        ),
    )
    def test_load_valid(self, data: dict[str, Any]) -> None:
        loaded = InFieldCDMLocationConfigYAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert dumped == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_error_messages(self, data: dict[str, Any], expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, InFieldCDMLocationConfigYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

    def test_load_valid_observation_view_config(self) -> None:
        data = {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "writeBack": {
                            "notificationsEndpointExternalId": "notif-endpoint",
                        },
                    },
                ],
            },
        }
        loaded = InFieldCDMLocationConfigYAML.model_validate(data)
        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert dumped == data

    def test_load_valid_observation_view_config_with_fields_config(self) -> None:
        data = {
            "externalId": "my_config",
            "space": "my_space",
            "viewMappings": {
                "observation": [
                    {
                        "view": {
                            "space": "my_space",
                            "version": "v1",
                            "externalId": "ObsView",
                        },
                        "fieldsConfig": {
                            "assets": {
                                "isRequired": True,
                                "isEditable": False,
                                "orderNumber": 1,
                            },
                            "files": {
                                "orderNumber": 2,
                            },
                        },
                        "writeBack": {
                            "notificationsEndpointExternalId": "notif-endpoint",
                        },
                    },
                ],
            },
        }
        loaded = InFieldCDMLocationConfigYAML.model_validate(data)
        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert dumped == data

    def test_load_valid_asset_properties_card_config(self) -> None:
        data = {
            "externalId": "my_config",
            "space": "my_space",
            "dataExplorationConfig": {
                "assetPropertiesCardConfig": {
                    "name": {
                        "displayName": "Asset name",
                        "orderNumber": 0,
                    },
                    "description": {
                        "orderNumber": 1,
                    },
                },
            },
        }
        loaded = InFieldCDMLocationConfigYAML.model_validate(data)
        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert dumped == data
