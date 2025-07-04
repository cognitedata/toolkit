from collections.abc import Iterable

import pytest
from _pytest.mark import ParameterSet
from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client.data_classes.capabilities import scope_intersection, scope_union
from cognite_toolkit._cdf_tk.resource_classes.capabilities import (
    AllScope,
    AppConfigScope,
    AssetRootIDScope,
    CurrentUserScope,
    DataSetScope,
    ExperimentScope,
    ExtractionPipelineScope,
    IDScope,
    IDScopeLowerCase,
    InstancesScope,
    LegacyDataModelScope,
    LegacySpaceScope,
    PartitionScope,
    PostgresGatewayUsersScope,
    SpaceIDScope,
    TableScope,
)


def scope_logic_test_cases() -> Iterable[ParameterSet]:
    # Basic cases - None handling
    yield pytest.param(None, None, None, None, id="Two None scopes")
    
    # AllScope cases
    all_scope = AllScope()
    yield pytest.param(all_scope, all_scope, all_scope, all_scope, id="Two identical AllScopes")
    
    # DataSetScope cases
    ds_scope_1 = DataSetScope(ids=["dataset1", "dataset2"])
    ds_scope_2 = DataSetScope(ids=["dataset2", "dataset3"])
    ds_scope_empty = DataSetScope(ids=[])
    ds_scope_intersection = DataSetScope(ids=["dataset2"])
    ds_scope_union = DataSetScope(ids=["dataset1", "dataset2", "dataset3"])
    
    yield pytest.param(ds_scope_1, ds_scope_2, ds_scope_intersection, ds_scope_union, 
                      id="DataSetScope intersection and union")
    yield pytest.param(ds_scope_1, ds_scope_1, ds_scope_1, ds_scope_1, 
                      id="Identical DataSetScopes")
    yield pytest.param(ds_scope_1, ds_scope_empty, ds_scope_empty, ds_scope_1, 
                      id="DataSetScope with empty scope")
    
    # IDScope cases
    id_scope_1 = IDScope(ids=["id1", "id2"])
    id_scope_2 = IDScope(ids=["id2", "id3"])
    id_scope_intersection = IDScope(ids=["id2"])
    id_scope_union = IDScope(ids=["id1", "id2", "id3"])
    
    yield pytest.param(id_scope_1, id_scope_2, id_scope_intersection, id_scope_union, 
                      id="IDScope intersection and union")
    
    # Mixed scope types with AllScope
    yield pytest.param(all_scope, ds_scope_1, ds_scope_1, all_scope, 
                      id="AllScope with DataSetScope")
    yield pytest.param(ds_scope_1, all_scope, ds_scope_1, all_scope, 
                      id="DataSetScope with AllScope")
    yield pytest.param(all_scope, id_scope_1, id_scope_1, all_scope, 
                      id="AllScope with IDScope")
    
    # SpaceIDScope cases
    space_scope_1 = SpaceIDScope(spaceIds=["space1", "space2"])  # type: ignore
    space_scope_2 = SpaceIDScope(spaceIds=["space2", "space3"])  # type: ignore
    space_scope_intersection = SpaceIDScope(spaceIds=["space2"])  # type: ignore
    space_scope_union = SpaceIDScope(spaceIds=["space1", "space2", "space3"])  # type: ignore
    
    yield pytest.param(space_scope_1, space_scope_2, space_scope_intersection, space_scope_union, 
                      id="SpaceIDScope intersection and union")
    
    # AssetRootIDScope cases
    asset_scope_1 = AssetRootIDScope(rootIds=["root1", "root2"])  # type: ignore
    asset_scope_2 = AssetRootIDScope(rootIds=["root2", "root3"])  # type: ignore
    asset_scope_intersection = AssetRootIDScope(rootIds=["root2"])  # type: ignore
    asset_scope_union = AssetRootIDScope(rootIds=["root1", "root2", "root3"])  # type: ignore
    
    yield pytest.param(asset_scope_1, asset_scope_2, asset_scope_intersection, asset_scope_union, 
                      id="AssetRootIDScope intersection and union")
    
    # ExtractionPipelineScope cases
    ep_scope_1 = ExtractionPipelineScope(ids=["ep1", "ep2"])
    ep_scope_2 = ExtractionPipelineScope(ids=["ep2", "ep3"])
    ep_scope_intersection = ExtractionPipelineScope(ids=["ep2"])
    ep_scope_union = ExtractionPipelineScope(ids=["ep1", "ep2", "ep3"])
    
    yield pytest.param(ep_scope_1, ep_scope_2, ep_scope_intersection, ep_scope_union, 
                      id="ExtractionPipelineScope intersection and union")
    
    # None cases with other scopes
    yield pytest.param(None, ds_scope_1, None, ds_scope_1, id="None with DataSetScope")
    yield pytest.param(ds_scope_1, None, None, ds_scope_1, id="DataSetScope with None")
    yield pytest.param(None, all_scope, None, all_scope, id="None with AllScope")
    yield pytest.param(all_scope, None, None, all_scope, id="AllScope with None")
    
    # Incompatible scope types (different scope types without AllScope)
    yield pytest.param(ds_scope_1, id_scope_1, None, None, 
                      id="Incompatible DataSetScope and IDScope")
    yield pytest.param(space_scope_1, asset_scope_1, None, None, 
                      id="Incompatible SpaceIDScope and AssetRootIDScope")
    
    # No common elements
    ds_scope_no_common = DataSetScope(ids=["dataset4", "dataset5"])
    yield pytest.param(ds_scope_1, ds_scope_no_common, ds_scope_empty, 
                      DataSetScope(ids=["dataset1", "dataset2", "dataset4", "dataset5"]), 
                      id="DataSetScope with no common elements")
    
    # Special scopes without common collection attributes
    current_user_scope = CurrentUserScope()
    yield pytest.param(current_user_scope, current_user_scope, current_user_scope, current_user_scope, 
                      id="CurrentUserScope identical")
    yield pytest.param(all_scope, current_user_scope, current_user_scope, all_scope, 
                      id="AllScope with CurrentUserScope")
    
    # Complex scopes
    table_scope_1 = TableScope(dbsToTables={"db1": ["table1", "table2"], "db2": ["table3"]})  # type: ignore
    table_scope_2 = TableScope(dbsToTables={"db1": ["table2", "table3"], "db3": ["table4"]})  # type: ignore
    
    yield pytest.param(table_scope_1, table_scope_2, None, None, 
                      id="TableScope complex intersection")
    
    # AppConfigScope
    app_scope_1 = AppConfigScope(apps=["SEARCH"])
    app_scope_2 = AppConfigScope(apps=["SEARCH"])
    
    yield pytest.param(app_scope_1, app_scope_2, app_scope_1, app_scope_1, 
                      id="AppConfigScope identical")


class TestScopeLogic:
    @pytest.mark.parametrize("scope1, scope2, intersection, union", list(scope_logic_test_cases()))
    def test_scope_intersection(
        self,
        scope1: Capability.Scope | None,
        scope2: Capability.Scope | None,
        intersection: Capability.Scope | None,
        union: Capability.Scope | None,
    ) -> None:
        result = scope_intersection(scope1, scope2)
        assert result == intersection

    @pytest.mark.parametrize("scope1, scope2, intersection, union", list(scope_logic_test_cases()))
    def test_scope_union(
        self,
        scope1: Capability.Scope | None,
        scope2: Capability.Scope | None,
        intersection: Capability.Scope | None,
        union: Capability.Scope | None,
    ) -> None:
        result = scope_union(scope1, scope2)
        assert result == union

    def test_all_scope_subclasses_supported(self) -> None:
        """Test that union and intersection methods support all subclasses of Capability.Scope"""
        # Create instances of all scope types
        scope_instances = [
            AllScope(),
            AppConfigScope(apps=["SEARCH"]),
            AssetRootIDScope(rootIds=["root1"]),  # type: ignore
            CurrentUserScope(),
            DataSetScope(ids=["ds1"]),
            ExperimentScope(experiments=["exp1"]),
            ExtractionPipelineScope(ids=["ep1"]),
            IDScope(ids=["id1"]),
            IDScopeLowerCase(ids=["id1"]),
            InstancesScope(instances=["inst1"]),
            LegacyDataModelScope(externalIds=["dm1"]),  # type: ignore
            LegacySpaceScope(externalIds=["space1"]),  # type: ignore
            PartitionScope(partitionIds=[1]),  # type: ignore
            PostgresGatewayUsersScope(usernames=["user1"]),
            SpaceIDScope(spaceIds=["space1"]),  # type: ignore
            TableScope(dbsToTables={"db1": ["table1"]}),  # type: ignore
        ]
        
        all_scope = AllScope()
        
        # Test that each scope type can be used in intersection and union operations
        for scope in scope_instances:
            # Test intersection with AllScope
            intersection_result = scope_intersection(scope, all_scope)  # type: ignore
            assert intersection_result == scope, f"Intersection of {type(scope).__name__} with AllScope failed"
            
            # Test union with AllScope
            union_result = scope_union(scope, all_scope)  # type: ignore
            assert union_result == all_scope, f"Union of {type(scope).__name__} with AllScope failed"
            
            # Test intersection with itself
            self_intersection = scope_intersection(scope, scope)
            assert self_intersection == scope, f"Self-intersection of {type(scope).__name__} failed"
            
            # Test union with itself
            self_union = scope_union(scope, scope)
            assert self_union == scope, f"Self-union of {type(scope).__name__} failed"
            
            # Test intersection with None
            none_intersection = scope_intersection(scope, None)
            assert none_intersection is None, f"Intersection of {type(scope).__name__} with None failed"
            
            # Test union with None
            none_union = scope_union(scope, None)
            assert none_union == scope, f"Union of {type(scope).__name__} with None failed"
