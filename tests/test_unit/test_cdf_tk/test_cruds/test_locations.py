import pytest
from cognite.client.data_classes.data_modeling.ids import DataModelId

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    AssetCentricFilter,
    AssetCentricSubFilter,
    LocationFilterScene,
    LocationFilterWrite,
)
from cognite_toolkit._cdf_tk.cruds._resource_cruds.location import LocationFilterCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import LOAD_DATA
from tests.test_unit.approval_client.client import ApprovalToolkitClient


@pytest.fixture
def exhaustive_filter(env_vars_with_client: EnvironmentVariables) -> LocationFilterWrite:
    loader = LocationFilterCRUD.create_loader(env_vars_with_client.get_client())
    raw_list = loader.load_resource_file(
        LOAD_DATA / "locations" / "exhaustive.LocationFilter.yaml", env_vars_with_client.dump()
    )
    loaded = loader.load_resource(raw_list[0], is_dry_run=False)
    return loaded


class TestLocationFilterLoader:
    def test_load_minimum_location_filter(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        loader = LocationFilterCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "locations" / "minimum.LocationFilter.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)
        assert isinstance(loaded, LocationFilterWrite)
        assert loaded.external_id == "springfield"

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

    def test_topological_sort_success(self) -> None:
        # Create location filters with parent-child relationships
        # Structure: grandparent -> parent -> child
        location_filters = [
            LocationFilterWrite(
                external_id="child",
                name="Child Location",
                parent_id=-1,  # Deferred lookup
                _parent_external_id="parent",
            ),
            LocationFilterWrite(
                external_id="parent",
                name="Parent Location",
                parent_id=-1,  # Deferred lookup
                _parent_external_id="grandparent",
            ),
            LocationFilterWrite(
                external_id="grandparent",
                name="Grandparent Location",
                parent_id=None,  # No parent
            ),
        ]

        sorted_filters = LocationFilterCRUD.topological_sort(location_filters)

        # Should be sorted with grandparent first, then parent, then child
        assert len(sorted_filters) == 3
        assert sorted_filters[0].external_id == "grandparent"
        assert sorted_filters[1].external_id == "parent"
        assert sorted_filters[2].external_id == "child"

    def test_topological_sort_raises_on_cycle(self) -> None:
        # Create location filters with circular dependencies
        dependencies = {
            "a": "b",
            "b": "c",
            "c": "a",  # This creates a cycle
        }

        location_filters = []
        for id_, parent_id in dependencies.items():
            location_filter = LocationFilterWrite(
                external_id=id_,
                name=f"Location {id_}",
                parent_id=-1,  # Deferred lookup
                _parent_external_id=parent_id,
            )
            location_filters.append(location_filter)

        with pytest.raises(ToolkitCycleError) as exc:
            LocationFilterCRUD.topological_sort(location_filters)

        error = exc.value
        assert isinstance(error, ToolkitCycleError)
        assert "cycle" in str(error).lower()
        assert "location filters" in str(error).lower()
