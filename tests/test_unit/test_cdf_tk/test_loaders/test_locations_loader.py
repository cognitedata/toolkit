from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling.ids import DataModelId

from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    AssetCentricFilter,
    AssetCentricSubFilter,
    LocationFilterScene,
    LocationFilterWrite,
)
from cognite_toolkit._cdf_tk.loaders._resource_loaders.location_loaders import LocationFilterLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import LOAD_DATA
from tests.test_unit.approval_client.client import ApprovalToolkitClient


class TestLocationFilterLoader:
    def test_load_minimum_location_filter(self, cognite_client_approval: ApprovalToolkitClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = cognite_client_approval.mock_client

        loader = LocationFilterLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(
            LOAD_DATA / "locations" / "minimum.LocationFilter.yaml", cdf_tool, skip_validation=False
        )
        assert isinstance(loaded, LocationFilterWrite)
        assert loaded.external_id == "springfield"

    @pytest.fixture
    def loaded(self, cognite_client_approval: ApprovalToolkitClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = cognite_client_approval.mock_client

        loader = LocationFilterLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(
            LOAD_DATA / "locations" / "exhaustive.LocationFilter.yaml", cdf_tool, skip_validation=False
        )
        return loaded

    def test_load_filter_write(self, loaded: LocationFilterWrite):
        assert isinstance(loaded, LocationFilterWrite)
        assert loaded.external_id == "unique-external-id-123"

    def test_load_filter_write_data_models(self, loaded: LocationFilterWrite):
        assert isinstance(loaded.data_models[0], DataModelId)
        assert loaded.data_models[0].external_id == "data-model-id-456"

    def test_load_filter_write_instance_spaces(self, loaded: LocationFilterWrite):
        assert isinstance(loaded.instance_spaces, list)
        assert loaded.instance_spaces[0] == "instance-space-main"

    def test_load_filter_write_scene(self, loaded: LocationFilterWrite):
        assert isinstance(loaded.scene, LocationFilterScene)
        assert loaded.scene.external_id == "scene-id-012"

    def test_load_filter_write_asset_centric(self, loaded: LocationFilterWrite):
        assert isinstance(loaded.asset_centric, AssetCentricFilter)
        assert isinstance(loaded.asset_centric.assets, AssetCentricSubFilter)
        assert loaded.asset_centric.asset_subtree_ids[0] == {"externalId": "general-subtree-id-890"}
        assert loaded.asset_centric.assets.asset_subtree_ids[0] == {"id": 345}
        assert loaded.asset_centric.events.asset_subtree_ids[0] == {"externalId": "event-subtree-id-678"}
