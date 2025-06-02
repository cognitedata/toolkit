from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.resource_classes import LocationYAML
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
                    "dataSetExternalId": ["dataset-001", "dataset-002"],
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
                        "dataSetExternalId": ["dataset-001"],
                        "assetSubtreeExternalIds": [{"externalId": "asset-001"}],
                        "externalIdPrefix": "asset-prefix",
                    },
                    "events": {
                        "dataSetExternalId": ["dataset-002"],
                        "assetSubtreeExternalIds": [{"externalId": "asset-002"}],
                        "externalIdPrefix": "event-prefix",
                    },
                    "timeseries": {"dataSetExternalId": ["dataset-003"], "externalIdPrefix": "ts-prefix"},
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


class TestLocationYAML:
    @pytest.mark.parametrize("data", list(find_resources("Location")))
    def test_load_valid_location(self, data: dict[str, object]) -> None:
        loaded = LocationYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("test_input", location_yaml_cases())
    def test_valid_location_variants(self, test_input: dict) -> None:
        location = LocationYAML.model_validate(test_input)
        assert location.model_dump(by_alias=True, exclude_unset=True) == test_input
