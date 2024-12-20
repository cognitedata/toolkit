from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling.ids import DataModelId

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    AssetCentricFilter,
    AssetCentricSubFilter,
    LocationFilterScene,
    LocationFilterWrite,
    LocationFilterWriteList,
)
from cognite_toolkit._cdf_tk.loaders._resource_loaders.location_loaders import LocationFilterLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import LOAD_DATA
from tests.test_unit.approval_client.client import ApprovalToolkitClient


@pytest.fixture
def exhaustive_filter(cdf_tool_mock: CDFToolConfig) -> LocationFilterWrite:
    loader = LocationFilterLoader.create_loader(cdf_tool_mock, None)
    loaded = loader.load_resource_file(
        LOAD_DATA / "locations" / "exhaustive.LocationFilter.yaml", cdf_tool_mock, skip_validation=False
    )
    assert len(loaded) == 1
    return loaded[0]


class TestLocationFilterLoader:
    def test_load_minimum_location_filter(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client

        loader = LocationFilterLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource_file(
            LOAD_DATA / "locations" / "minimum.LocationFilter.yaml", cdf_tool, skip_validation=False
        )
        assert isinstance(loaded, LocationFilterWriteList)
        assert len(loaded) == 1
        first = loaded[0]
        assert isinstance(first, LocationFilterWrite)
        assert first.external_id == "springfield"

    def test_load_filter_write(self, exhaustive_filter: LocationFilterWrite) -> None:
        assert isinstance(exhaustive_filter, LocationFilterWrite)
        assert exhaustive_filter.external_id == "unique-external-id-123"

    def test_load_filter_write_data_models(self, exhaustive_filter: LocationFilterWrite) -> None:
        assert isinstance(exhaustive_filter.data_models[0], DataModelId)
        assert exhaustive_filter.data_models[0].external_id == "data-model-id-456"

    def test_load_filter_write_instance_spaces(self, exhaustive_filter: LocationFilterWrite) -> None:
        assert isinstance(exhaustive_filter.instance_spaces, list)
        assert exhaustive_filter.instance_spaces[0] == "instance-space-main"

    def test_load_filter_write_scene(self, exhaustive_filter: LocationFilterWrite) -> None:
        assert isinstance(exhaustive_filter.scene, LocationFilterScene)
        assert exhaustive_filter.scene.external_id == "scene-id-012"

    def test_load_filter_write_asset_centric(self, exhaustive_filter: LocationFilterWrite) -> None:
        assert isinstance(exhaustive_filter.asset_centric, AssetCentricFilter)
        assert isinstance(exhaustive_filter.asset_centric.assets, AssetCentricSubFilter)
        assert exhaustive_filter.asset_centric.asset_subtree_ids[0] == {"externalId": "general-subtree-id-890"}
        assert exhaustive_filter.asset_centric.assets.asset_subtree_ids[0] == {"externalId": "root-asset"}
        assert exhaustive_filter.asset_centric.events.asset_subtree_ids[0] == {"externalId": "event-subtree-id-678"}
