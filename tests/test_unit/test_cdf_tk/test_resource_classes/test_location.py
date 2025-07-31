from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import LocationYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def location_yaml_cases() -> Iterable:
    yaml_cases = [
        # Basic case with only required fields
        {"BasicCase": {"externalId": "loc-001", "name": "Test Location"}},
        # With description and parent
        {
            "ParentCase": {
                "externalId": "loc-002",
                "name": "Child Location",
                "description": "Test description",
                "parentExternalId": "loc-001",
            }
        },
        # With scene
        {
            "SceneCase": {
                "externalId": "loc-003",
                "name": "Location with Scene",
                "scene": {"externalId": "scene-001", "space": "test-space"},
            }
        },
        # With data models
        {
            "DataModelCase": {
                "externalId": "loc-004",
                "name": "Location with Data Models",
                "dataModels": [
                    {"externalId": "model-001", "space": "test-space", "version": "1.0"},
                    {"externalId": "model-002", "space": "test-space", "version": "2.0"},
                ],
            }
        },
        # With instance spaces
        {
            "InstanceSpacesCase": {
                "externalId": "loc-005",
                "name": "Location with Instance Spaces",
                "instanceSpaces": ["space-1", "space-2"],
            }
        },
        # With views
        {
            "ViewsCase": {
                "externalId": "loc-006",
                "name": "Location with Views",
                "views": [
                    {"externalId": "view-001", "space": "test-space", "version": "1.0", "representsEntity": "ASSET"},
                    {
                        "externalId": "view-002",
                        "space": "test-space",
                        "version": "1.0",
                        "representsEntity": "NOTIFICATION",
                    },
                ],
            }
        },
        # With asset centric resources - simple case
        {
            "SimpleAssetCentricCase": {
                "externalId": "loc-007",
                "name": "Location with Asset Centric Resource",
                "assetCentric": {
                    "dataSetExternalIds": ["dataset-001", "dataset-002"],
                    "assetSubtreeExternalIds": [{"externalId": "asset-001"}, {"externalId": "asset-002"}],
                    "externalIdPrefix": "test-prefix",
                },
            }
        },
        # With asset centric resources - full case
        {
            "FullAssetCentricCase": {
                "externalId": "loc-008",
                "name": "Location with Full Asset Centric Resources",
                "assetCentric": {
                    "assets": {
                        "dataSetExternalIds": ["dataset-001"],
                        "assetSubtreeExternalIds": [{"externalId": "asset-001"}],
                        "externalIdPrefix": "asset-prefix",
                    },
                    "events": {
                        "dataSetExternalIds": ["dataset-002"],
                        "assetSubtreeExternalIds": [{"externalId": "asset-002"}],
                        "externalIdPrefix": "event-prefix",
                    },
                    "timeseries": {"dataSetExternalIds": ["dataset-003"], "externalIdPrefix": "ts-prefix"},
                    "files": {
                        "assetSubtreeExternalIds": [{"externalId": "asset-003"}],
                    },
                    "sequences": {"externalIdPrefix": "seq-prefix"},
                },
            }
        },
        # Full example with all fields
        {
            "CompleteCase": {
                "externalId": "loc-009",
                "name": "Full Location Example",
                "description": "Complete location with all fields",
                "parentExternalId": "loc-001",
                "dataModels": [{"externalId": "model-001", "space": "test-space", "version": "1.0"}],
                "instanceSpaces": ["space-1", "space-2"],
                "scene": {"externalId": "scene-001", "space": "test-space"},
                "views": [
                    {"externalId": "view-001", "space": "test-space", "version": "1.0", "representsEntity": "ASSET"}
                ],
                "assetCentric": {
                    "assets": {"externalIdPrefix": "asset-prefix"},
                    "events": {"externalIdPrefix": "event-prefix"},
                },
            }
        },
    ]

    yield from (pytest.param(next(iter(case.values())), id=next(iter(case.keys()))) for case in yaml_cases)


def invalid_location_filters_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "Location 1"}, {"Missing required field: 'externalId'"}, id="Missing required field: externalId"
    )
    yield pytest.param(
        {"externalId": "my_location", "name": "Location 1", "dataModelingType": "ASSET_CENTRIC_ONLY"},
        {"In field dataModelingType input should be 'HYBRID' or 'DATA_MODELING_ONLY'. Got 'ASSET_CENTRIC_ONLY'."},
        id="Invalid dataModelingType value",
    )
    yield pytest.param(
        {"externalId": "location_1", "name": "Location 1", "dataModels": ["model-1", "model-2"]},
        {
            "In dataModels[1] input must be an object of type DataModelID. Got 'model-1' of type str.",
            "In dataModels[2] input must be an object of type DataModelID. Got 'model-2' of type str.",
        },
        id="Invalid list of dataModels.",
    )


class TestLocationYAML:
    @pytest.mark.parametrize("data", list(find_resources("Location")))
    def test_load_valid_location(self, data: dict[str, object]) -> None:
        loaded = LocationYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("test_input", location_yaml_cases())
    def test_valid_location_variants(self, test_input: dict) -> None:
        location = LocationYAML.model_validate(test_input)
        assert location.model_dump(by_alias=True, exclude_unset=True) == test_input

    @pytest.mark.parametrize("data, expected_errors", list(invalid_location_filters_test_cases()))
    def test_invalid_location_filters_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, LocationYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
