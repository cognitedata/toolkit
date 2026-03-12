from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.group.scope_logic import (
    scope_difference,
    scope_intersection,
    scope_union,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group.scopes import (
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
    PartitionScope,
    PostgresGatewayUsersScope,
    ScopeDefinition,
    SpaceIDScope,
    TableScope,
    UnknownScope,
)
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from tests.test_unit.utils import FakeCogniteResourceGenerator


def scope_logic_test_cases() -> Iterable:
    # AllScope cases
    all_scope = AllScope()
    yield pytest.param(all_scope, all_scope, all_scope, all_scope, id="Two identical AllScopes")

    # DataSetScope cases
    ds_scope_1 = DataSetScope(ids=[123, 456])
    ds_scope_2 = DataSetScope(ids=[456, 789])
    ds_scope_3 = DataSetScope(ids=[42])

    yield pytest.param(
        ds_scope_1,
        ds_scope_2,
        DataSetScope(ids=[456]),
        DataSetScope(ids=[123, 456, 789]),
        id="DataSetScope intersection and union",
    )
    yield pytest.param(ds_scope_1, ds_scope_1, ds_scope_1, ds_scope_1, id="Identical DataSetScopes")
    yield pytest.param(
        ds_scope_1,
        ds_scope_3,
        None,
        DataSetScope(ids=[42, 123, 456]),
        id="DataSetScope with no intersection",
    )

    # IDScope cases
    yield pytest.param(
        IDScope(ids=[123, 456]),
        IDScope(ids=[456, 789]),
        IDScope(ids=[456]),
        IDScope(ids=[123, 456, 789]),
        id="IDScope intersection and union",
    )

    # SpaceIDScope cases
    yield pytest.param(
        SpaceIDScope(space_ids=["space1", "space2"]),
        SpaceIDScope(space_ids=["space2", "space3"]),
        SpaceIDScope(space_ids=["space2"]),
        SpaceIDScope(space_ids=["space1", "space2", "space3"]),
        id="SpaceIDScope intersection and union",
    )

    # AssetRootIDScope cases
    yield pytest.param(
        AssetRootIDScope(root_ids=[123, 456]),
        AssetRootIDScope(root_ids=[456, 789]),
        AssetRootIDScope(root_ids=[456]),
        AssetRootIDScope(root_ids=[123, 456, 789]),
        id="AssetRootIDScope intersection and union",
    )

    # ExtractionPipelineScope cases
    yield pytest.param(
        ExtractionPipelineScope(ids=[123, 456]),
        ExtractionPipelineScope(ids=[456, 789]),
        ExtractionPipelineScope(ids=[456]),
        ExtractionPipelineScope(ids=[123, 456, 789]),
        id="ExtractionPipelineScope intersection and union",
    )

    # CurrentUserScope cases
    current_user_scope = CurrentUserScope()
    yield pytest.param(
        current_user_scope,
        current_user_scope,
        current_user_scope,
        current_user_scope,
        id="CurrentUserScope identical",
    )

    # TableScope cases
    yield pytest.param(
        TableScope(dbs_to_tables={"db1": ["table1", "table2"], "db2": ["table3"]}),
        TableScope(dbs_to_tables={"db1": ["table2", "table3"], "db3": ["table4"]}),
        TableScope(dbs_to_tables={"db1": ["table2"]}),
        TableScope(dbs_to_tables={"db1": ["table1", "table2", "table3"], "db2": ["table3"], "db3": ["table4"]}),
        id="TableScope complex intersection and union",
    )
    yield pytest.param(
        TableScope(dbs_to_tables={"db1": ["table1"]}),
        TableScope(dbs_to_tables={"db2": ["table1"]}),
        None,
        TableScope(dbs_to_tables={"db1": ["table1"], "db2": ["table1"]}),
        id="TableScope no intersection",
    )

    # AppConfigScope cases
    yield pytest.param(
        AppConfigScope(apps=["SEARCH"]),
        AppConfigScope(apps=["SEARCH"]),
        AppConfigScope(apps=["SEARCH"]),
        AppConfigScope(apps=["SEARCH"]),
        id="AppConfigScope identical",
    )

    # InstancesScope cases
    yield pytest.param(
        InstancesScope(instances=["inst1", "inst2"]),
        InstancesScope(instances=["inst2", "inst3"]),
        InstancesScope(instances=["inst2"]),
        InstancesScope(instances=["inst1", "inst2", "inst3"]),
        id="InstancesScope intersection and union",
    )

    # PartitionScope cases
    yield pytest.param(
        PartitionScope(partition_ids=[1, 2]),
        PartitionScope(partition_ids=[2, 3]),
        PartitionScope(partition_ids=[2]),
        PartitionScope(partition_ids=[1, 2, 3]),
        id="PartitionScope intersection and union",
    )

    # ExperimentScope cases
    yield pytest.param(
        ExperimentScope(experiments=["exp1", "exp2"]),
        ExperimentScope(experiments=["exp2", "exp3"]),
        ExperimentScope(experiments=["exp2"]),
        ExperimentScope(experiments=["exp1", "exp2", "exp3"]),
        id="ExperimentScope intersection and union",
    )

    # PostgresGatewayUsersScope cases
    yield pytest.param(
        PostgresGatewayUsersScope(usernames=["user1", "user2"]),
        PostgresGatewayUsersScope(usernames=["user2", "user3"]),
        PostgresGatewayUsersScope(usernames=["user2"]),
        PostgresGatewayUsersScope(usernames=["user1", "user2", "user3"]),
        id="PostgresGatewayUsersScope intersection and union",
    )

    # IDScopeLowerCase cases
    yield pytest.param(
        IDScopeLowerCase(ids=[1, 2]),
        IDScopeLowerCase(ids=[2, 3]),
        IDScopeLowerCase(ids=[2]),
        IDScopeLowerCase(ids=[1, 2, 3]),
        id="IDScopeLowerCase intersection and union",
    )


class TestScopeLogic:
    @pytest.mark.parametrize("scope1, scope2, intersection, union", list(scope_logic_test_cases()))
    def test_scope_intersection(
        self,
        scope1: ScopeDefinition,
        scope2: ScopeDefinition,
        intersection: ScopeDefinition | None,
        union: ScopeDefinition,
    ) -> None:
        assert scope_intersection(scope1, scope2) == intersection

    @pytest.mark.parametrize("scope1, scope2, intersection, union", list(scope_logic_test_cases()))
    def test_scope_union(
        self,
        scope1: ScopeDefinition,
        scope2: ScopeDefinition,
        intersection: ScopeDefinition | None,
        union: ScopeDefinition,
    ) -> None:
        assert scope_union(scope1, scope2) == union

    @pytest.mark.parametrize(
        "scope1, scope2, difference",
        [
            pytest.param(
                DataSetScope(ids=[1, 2, 3]),
                DataSetScope(ids=[2, 3, 4]),
                DataSetScope(ids=[1]),
                id="DataSetScope difference",
            ),
            pytest.param(
                IDScope(ids=[10, 20]),
                IDScope(ids=[20, 30]),
                IDScope(ids=[10]),
                id="IDScope difference",
            ),
            pytest.param(
                SpaceIDScope(space_ids=["a", "b"]),
                SpaceIDScope(space_ids=["b", "c"]),
                SpaceIDScope(space_ids=["a"]),
                id="SpaceIDScope difference",
            ),
            pytest.param(
                AllScope(),
                DataSetScope(ids=[1, 2]),
                AllScope(),
                id="AllScope difference with specific scope",
            ),
            pytest.param(
                DataSetScope(ids=[1, 2, 3]),
                AllScope(),
                None,
                id="Specific scope difference with AllScope",
            ),
            pytest.param(
                AllScope(),
                AllScope(),
                None,
                id="AllScope difference with AllScope yields None",
            ),
            pytest.param(
                DataSetScope(ids=[1, 2, 3]),
                DataSetScope(ids=[1, 2, 3]),
                None,
                id="Identical DataSetScopes yield None",
            ),
            pytest.param(
                DataSetScope(ids=[1, 2]),
                DataSetScope(ids=[1, 2, 3]),
                None,
                id="Subset difference yields None",
            ),
            pytest.param(
                TableScope(dbs_to_tables={"db1": ["t1", "t2"], "db2": ["t3"]}),
                TableScope(dbs_to_tables={"db1": ["t2"], "db3": ["t4"]}),
                TableScope(dbs_to_tables={"db1": ["t1"], "db2": ["t3"]}),
                id="TableScope difference",
            ),
            pytest.param(
                TableScope(dbs_to_tables={"db1": ["t1"]}),
                TableScope(dbs_to_tables={"db1": ["t1"]}),
                None,
                id="Identical TableScope yields None",
            ),
            pytest.param(
                DataSetScope(ids=[1, 2]),
                None,
                DataSetScope(ids=[1, 2]),
                id="Difference with None returns scope1",
            ),
        ],
    )
    def test_scope_difference(
        self,
        scope1: ScopeDefinition,
        scope2: ScopeDefinition,
        difference: ScopeDefinition | None,
    ) -> None:
        assert scope_difference(scope1, scope2) == difference

    def test_raises_unknown_scope_intersection(self) -> None:
        instance = UnknownScope(scope_name="mystery")
        with pytest.raises(TypeError, match="Cannot intersect unknown scopes"):
            scope_intersection(instance, instance)

    def test_raises_unknown_scope_difference(self) -> None:
        instance = UnknownScope(scope_name="mystery")
        with pytest.raises(TypeError, match="Cannot difference unknown scopes"):
            scope_difference(instance, instance)

    def test_raise_different_scope_types_intersection(self) -> None:
        with pytest.raises(ValueError, match="Cannot intersect scopes of different types"):
            scope_intersection(DataSetScope(ids=[1, 2]), IDScope(ids=[3, 4]))

    def test_raise_different_scope_types_union(self) -> None:
        with pytest.raises(ValueError, match="Cannot union scopes of different types"):
            scope_union(DataSetScope(ids=[1, 2]), IDScope(ids=[3, 4]))

    def test_raise_different_scope_types_difference(self) -> None:
        with pytest.raises(ValueError, match="Cannot difference scopes of different types"):
            scope_difference(DataSetScope(ids=[1, 2]), IDScope(ids=[3, 4]))

    def test_current_user_scope_difference_yields_none(self) -> None:
        assert scope_difference(CurrentUserScope(), CurrentUserScope()) is None

    @pytest.mark.parametrize(
        "scope1, scope2",
        [
            (DataSetScope(ids=[1, 2]), DataSetScope(ids=[3, 4])),
            (IDScope(ids=[10]), IDScope(ids=[20])),
            (SpaceIDScope(space_ids=["a"]), SpaceIDScope(space_ids=["b"])),
            (AssetRootIDScope(root_ids=[100]), AssetRootIDScope(root_ids=[200])),
            (ExtractionPipelineScope(ids=[5]), ExtractionPipelineScope(ids=[6])),
            (TableScope(dbs_to_tables={"db1": ["t1"]}), TableScope(dbs_to_tables={"db2": ["t2"]})),
            (AppConfigScope(apps=["A"]), AppConfigScope(apps=["B"])),
            (ExperimentScope(experiments=["exp1"]), ExperimentScope(experiments=["exp2"])),
            (PartitionScope(partition_ids=[1]), PartitionScope(partition_ids=[2])),
            (InstancesScope(instances=["inst1"]), InstancesScope(instances=["inst2"])),
            (PostgresGatewayUsersScope(usernames=["user1"]), PostgresGatewayUsersScope(usernames=["user2"])),
            (IDScopeLowerCase(ids=[1]), IDScopeLowerCase(ids=[2])),
        ],
    )
    def test_scope_no_intersection(self, scope1: ScopeDefinition, scope2: ScopeDefinition) -> None:
        assert scope_intersection(scope1, scope2) is None

    def test_intersection_no_args_returns_none(self) -> None:
        assert scope_intersection() is None

    def test_union_no_args_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one scope is required"):
            scope_union()

    def test_intersection_single_scope(self) -> None:
        scope = DataSetScope(ids=[1, 2])
        assert scope_intersection(scope) == scope

    def test_union_single_scope(self) -> None:
        scope = DataSetScope(ids=[1, 2])
        assert scope_union(scope) == scope

    def test_intersection_three_scopes(self) -> None:
        result = scope_intersection(
            DataSetScope(ids=[1, 2, 3]),
            DataSetScope(ids=[2, 3, 4]),
            DataSetScope(ids=[3, 4, 5]),
        )
        assert result == DataSetScope(ids=[3])

    def test_union_three_scopes(self) -> None:
        result = scope_union(
            DataSetScope(ids=[1, 2]),
            DataSetScope(ids=[3, 4]),
            DataSetScope(ids=[5, 6]),
        )
        assert result == DataSetScope(ids=[1, 2, 3, 4, 5, 6])

    def test_allscope_intersection_is_identity(self) -> None:
        scope = SpaceIDScope(space_ids=["space1", "space2"])
        assert scope_intersection(AllScope(), scope) == scope
        assert scope_intersection(scope, AllScope()) == scope
        assert scope_intersection(AllScope(), scope, AllScope()) == scope

    def test_allscope_union_is_absorbing(self) -> None:
        scope = SpaceIDScope(space_ids=["space1", "space2"])
        assert scope_union(AllScope(), scope) == AllScope()
        assert scope_union(scope, AllScope()) == AllScope()
        assert scope_union(scope, DataSetScope(ids=[1]), AllScope()) == AllScope()

    def test_scope_union_of_unknown_scopes(self) -> None:
        scope1 = UnknownScope.model_validate(dict(scope_name="mystery", someField=[1, 2]))
        scope2 = UnknownScope.model_validate(dict(scope_name="mystery", someField=[2, 3]))
        union = scope_union(scope1, scope2)
        assert union.dump() == {"someField": [1, 2, 3]}

    def test_scope_union_unknown_unhashable(self) -> None:
        scope1 = UnknownScope.model_validate(dict(scope_name="mystery", someField=[{"key": [1]}]))
        scope2 = UnknownScope.model_validate(dict(scope_name="mystery", someField=[{"key": [2]}]))
        with pytest.raises(TypeError, match="Cannot union unknown scopes with unhashable fields"):
            scope_union(scope1, scope2)


class TestScopes:
    @pytest.mark.parametrize("scope_cls", get_concrete_subclasses(ScopeDefinition))
    def test_scope_is_hashable(self, scope_cls: type[ScopeDefinition]) -> None:
        """Ensure all ScopeDefinition subclasses are hashable."""
        instance = FakeCogniteResourceGenerator(seed=42).create_instance(scope_cls)
        assert isinstance(instance, ScopeDefinition)
        assert isinstance(hash(instance), int)

    @pytest.mark.parametrize(
        "scope, other, are_equal",
        [
            pytest.param(DataSetScope(ids=[1, 2]), DataSetScope(ids=[1, 2]), True, id="Identical DataSetScopes"),
            pytest.param(DataSetScope(ids=[1, 2]), DataSetScope(ids=[2, 3]), False, id="Different DataSetScopes"),
            pytest.param(
                TableScope(dbs_to_tables={"db1": []}),
                TableScope(dbs_to_tables={"db1": []}),
                True,
                id="Identical empty TableScopes",
            ),
            pytest.param(
                TableScope(dbs_to_tables={"db1": []}),
                TableScope(dbs_to_tables={"db1": ["table1"]}),
                False,
                id="Empty vs non-empty TableScope",
            ),
            pytest.param(
                TableScope(dbs_to_tables={"db1": ["table1", "table2"]}),
                TableScope(dbs_to_tables={"db1": ["table2", "table1"]}),
                True,
                id="TableScopes with same tables in different order",
            ),
        ],
    )
    def test_scope_equality(self, scope: ScopeDefinition, other: ScopeDefinition, are_equal: bool) -> None:
        """Test equality and inequality of ScopeDefinition instances."""
        if are_equal:
            assert scope == other
            assert hash(scope) == hash(other)
        else:
            assert scope != other
