from collections.abc import Iterable

import pytest
from cognite.client.data_classes.capabilities import (
    AllScope,
    AppConfigScope,
    AssetRootIDScope,
    Capability,
    CurrentUserScope,
    DataSetScope,
    ExperimentsScope,
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
    UnknownScope,
)

from cognite_toolkit._cdf_tk.client.data_classes.capabilities import scope_intersection, scope_union
from tests.test_unit.utils import FakeCogniteResourceGenerator


def scope_logic_test_cases() -> Iterable:
    yield pytest.param(DataSetScope(ids=[1]), None, None, DataSetScope(ids=[1]), id="One None scope")

    # AllScope cases
    all_scope = AllScope()
    yield pytest.param(all_scope, all_scope, all_scope, all_scope, id="Two identical AllScopes")

    # DataSetScope cases
    ds_scope_1 = DataSetScope(ids=[123, 456])
    ds_scope_2 = DataSetScope(ids=[456, 789])
    ds_scope_3 = DataSetScope(ids=[42])
    ds_scope_intersection = DataSetScope(ids=[456])
    ds_scope_union = DataSetScope(ids=[123, 456, 789])

    yield pytest.param(
        ds_scope_1, ds_scope_2, ds_scope_intersection, ds_scope_union, id="DataSetScope intersection and union"
    )
    yield pytest.param(ds_scope_1, ds_scope_1, ds_scope_1, ds_scope_1, id="Identical DataSetScopes")
    yield pytest.param(
        ds_scope_1, ds_scope_3, None, DataSetScope([42, 123, 456]), id="DataSetScope with no intersection"
    )

    # IDScope cases
    id_scope_1 = IDScope(ids=[123, 456])
    id_scope_2 = IDScope(ids=[456, 789])
    id_scope_intersection = IDScope(ids=[456])
    id_scope_union = IDScope(ids=[123, 456, 789])

    yield pytest.param(
        id_scope_1, id_scope_2, id_scope_intersection, id_scope_union, id="IDScope intersection and union"
    )

    # SpaceIDScope cases
    space_scope_1 = SpaceIDScope(space_ids=["space1", "space2"])
    space_scope_2 = SpaceIDScope(space_ids=["space2", "space3"])
    space_scope_intersection = SpaceIDScope(space_ids=["space2"])
    space_scope_union = SpaceIDScope(space_ids=["space1", "space2", "space3"])

    yield pytest.param(
        space_scope_1,
        space_scope_2,
        space_scope_intersection,
        space_scope_union,
        id="SpaceIDScope intersection and union",
    )

    # AssetRootIDScope cases
    asset_scope_1 = AssetRootIDScope(root_ids=[123, 456])
    asset_scope_2 = AssetRootIDScope(root_ids=[456, 789])
    asset_scope_intersection = AssetRootIDScope(root_ids=[456])
    asset_scope_union = AssetRootIDScope(root_ids=[123, 456, 789])

    yield pytest.param(
        asset_scope_1,
        asset_scope_2,
        asset_scope_intersection,
        asset_scope_union,
        id="AssetRootIDScope intersection and union",
    )

    # ExtractionPipelineScope cases
    ep_scope_1 = ExtractionPipelineScope(ids=[123, 456])
    ep_scope_2 = ExtractionPipelineScope(ids=[456, 789])
    ep_scope_intersection = ExtractionPipelineScope(ids=[456])
    ep_scope_union = ExtractionPipelineScope(ids=[123, 456, 789])

    yield pytest.param(
        ep_scope_1,
        ep_scope_2,
        ep_scope_intersection,
        ep_scope_union,
        id="ExtractionPipelineScope intersection and union",
    )

    # Special scopes without common collection attributes
    current_user_scope = CurrentUserScope()
    yield pytest.param(
        current_user_scope, current_user_scope, current_user_scope, current_user_scope, id="CurrentUserScope identical"
    )

    # Complex scopes
    table_scope_1 = TableScope(dbs_to_tables={"db1": ["table1", "table2"], "db2": ["table3"]})
    table_scope_2 = TableScope(dbs_to_tables={"db1": ["table2", "table3"], "db3": ["table4"]})

    yield pytest.param(
        table_scope_1,
        table_scope_2,
        TableScope(
            dbs_to_tables={"db1": ["table2"]},
        ),
        TableScope(
            dbs_to_tables={
                "db1": ["table1", "table2", "table3"],
                "db2": ["table3"],
                "db3": ["table4"],
            }
        ),
        id="TableScope complex intersection",
    )
    yield pytest.param(
        TableScope(dbs_to_tables={"db1": ["table1"]}),
        TableScope(dbs_to_tables={"db2": ["table1"]}),
        None,
        TableScope(dbs_to_tables={"db1": ["table1"], "db2": ["table1"]}),
        id="TableScope no intersection",
    )

    # AppConfigScope
    app_scope_1 = AppConfigScope(apps=["SEARCH"])
    app_scope_2 = AppConfigScope(apps=["SEARCH"])

    yield pytest.param(app_scope_1, app_scope_2, app_scope_1, app_scope_1, id="AppConfigScope identical")


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

    @pytest.mark.parametrize(
        "scope_cls", [scope_cls for scope_cls in Capability.Scope.__subclasses__() if scope_cls is not UnknownScope]
    )
    def test_support_all_scopes(self, scope_cls: type[Capability.Scope]) -> None:
        """Ensure all scope classes are supported by the intersection and union functions."""
        instance = FakeCogniteResourceGenerator(seed=42).create_instance(scope_cls)

        # Checking that no exceptions are raised for any scope type
        _ = scope_intersection(instance, instance)
        _ = scope_union(instance, instance)

    def test_raises_unknown_scope_intersection(self) -> None:
        instance = FakeCogniteResourceGenerator(seed=42).create_instance(UnknownScope)
        with pytest.raises(TypeError, match="Unknown scope type"):
            scope_intersection(instance, instance)

    def test_raises_unknown_scope_union(self) -> None:
        instance = FakeCogniteResourceGenerator(seed=42).create_instance(UnknownScope)
        with pytest.raises(TypeError, match="Unknown scope type"):
            scope_union(instance, instance)

    def test_raise_different_scope_types_intersection(self) -> None:
        with pytest.raises(ValueError, match="Cannot intersect scopes of different types"):
            scope_intersection(DataSetScope(ids=[1, 2]), IDScope(ids=[3, 4]))

    def test_raise_different_scope_types_union(self) -> None:
        with pytest.raises(ValueError, match="Cannot union scopes of different types"):
            scope_union(DataSetScope(ids=[1, 2]), IDScope(ids=[3, 4]))

    @pytest.mark.parametrize(
        "scope1, scope2",
        [
            (DataSetScope(ids=[1, 2]), DataSetScope(ids=[3, 4])),
            (IDScope(ids=[10]), IDScope(ids=[20])),
            (SpaceIDScope(space_ids=["a"]), SpaceIDScope(space_ids=["b"])),
            (AssetRootIDScope(root_ids=[100]), AssetRootIDScope(root_ids=[200])),
            (ExtractionPipelineScope(ids=[5]), ExtractionPipelineScope(ids=[6])),
            (
                TableScope(dbs_to_tables={"db1": ["t1"]}),
                TableScope(dbs_to_tables={"db2": ["t2"]}),
            ),
            (AppConfigScope(apps=["A"]), AppConfigScope(apps=["B"])),
            (ExperimentsScope(experiments=["exp1"]), ExperimentsScope(experiments=["exp2"])),
            (LegacySpaceScope(external_ids=["space1"]), LegacySpaceScope(external_ids=["space2"])),
            (LegacyDataModelScope(external_ids=["model1"]), LegacyDataModelScope(external_ids=["model2"])),
            (PartitionScope(partition_ids=[1]), PartitionScope(partition_ids=[2])),
            (InstancesScope(instances=["inst1"]), InstancesScope(instances=["inst2"])),
            (PostgresGatewayUsersScope(usernames=["user1"]), PostgresGatewayUsersScope(usernames=["user2"])),
            (IDScopeLowerCase(ids=[1]), IDScopeLowerCase(ids=[2])),
        ],
    )
    def test_scope_no_intersection(self, scope1: Capability.Scope, scope2: Capability.Scope) -> None:
        assert scope_intersection(scope1, scope2) is None
