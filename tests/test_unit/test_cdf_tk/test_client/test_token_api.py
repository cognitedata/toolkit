from collections.abc import Iterable

import pytest
from cognite.client.data_classes.capabilities import (
    AllScope,
    AssetRootIDScope,
    AssetsAcl,
    Capability,
    DataModelInstancesAcl,
    DataSetScope,
    IDScopeLowerCase,
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsScope,
    SpaceIDScope,
    TimeSeriesAcl,
)
from cognite.client.data_classes.iam import ProjectSpec, TokenInspection

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client


def get_scope_test_cases() -> Iterable:
    yield pytest.param(
        [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([123]))],
        [TimeSeriesAcl.Action.Read],
        [DataSetScope(ids=[123])],
        id="TimeSeriesAcl with DataSet scope",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All()),
            TimeSeriesAcl([TimeSeriesAcl.Action.Write], TimeSeriesAcl.Scope.DataSet([123])),
        ],
        [TimeSeriesAcl.Action.Read],
        [AllScope()],
        id="Prefer all scope over DataSet scope",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All()),
            TimeSeriesAcl([TimeSeriesAcl.Action.Write], TimeSeriesAcl.Scope.DataSet([123])),
        ],
        [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
        [TimeSeriesAcl.Scope.DataSet([123])],
        id="Minimum scope for multiple actions",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([1, 2])),
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([2, 3])),
        ],
        [TimeSeriesAcl.Action.Read],
        [DataSetScope(ids=[1, 2, 3])],
        id="Take union of scopes for same action",
    )
    yield pytest.param(
        [DataModelInstancesAcl([DataModelInstancesAcl.Action.Read], DataModelInstancesAcl.Scope.All())],
        [DataModelInstancesAcl.Action.Write],
        None,
        id="DataModelInstancesAcl with All scope and no matching action",
    )
    yield pytest.param(
        [DataModelInstancesAcl([DataModelInstancesAcl.Action.Write], DataModelInstancesAcl.Scope.SpaceID(["space1"]))],
        [DataModelInstancesAcl.Action.Write],
        [SpaceIDScope(space_ids=["space1"])],
        id="DataModelInstancesAcl with SpaceID scope",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([123])),
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.ID([789])),
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.AssetRootID([456])),
        ],
        [TimeSeriesAcl.Action.Read],
        [
            AssetRootIDScope(root_ids=[456]),
            DataSetScope(ids=[123]),
            IDScopeLowerCase(ids=[789]),
        ],
        id="Multiple different scopes with AssetsAcl action",
    )
    yield pytest.param(
        [],
        [TimeSeriesAcl.Action.Read],
        None,
        id="No capabilities present",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([1])),
            TimeSeriesAcl([TimeSeriesAcl.Action.Write], TimeSeriesAcl.Scope.DataSet([2])),
        ],
        [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
        None,
        id="Multiple actions, no overlapping scope",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All()),
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([1])),
        ],
        [TimeSeriesAcl.Action.Read],
        [AllScope()],
        id="AllScope present with other scopes",
    )
    yield pytest.param(
        [
            TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All()),
        ],
        [AssetsAcl.Action.Read],
        None,
        id="Unknown action class",
    )
    yield pytest.param(
        [
            DataModelInstancesAcl(
                [DataModelInstancesAcl.Action.Read], DataModelInstancesAcl.Scope.SpaceID(["space1", "space2", "space3"])
            ),
            DataModelInstancesAcl(
                [DataModelInstancesAcl.Action.Write],
                DataModelInstancesAcl.Scope.SpaceID(["space2", "space4", "space5"]),
            ),
            DataModelInstancesAcl(
                [DataModelInstancesAcl.Action.Write_Properties],
                DataModelInstancesAcl.Scope.SpaceID(["space2", "space3", "space6"]),
            ),
        ],
        [
            DataModelInstancesAcl.Action.Read,
            DataModelInstancesAcl.Action.Write,
            DataModelInstancesAcl.Action.Write_Properties,
        ],
        [SpaceIDScope(space_ids=["space2"])],
        id="DataModelInstancesAcl with multiple actions and SpaceID scope intersection",
    )
    yield pytest.param(
        [
            AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.DataSet([1])),
            AssetsAcl([AssetsAcl.Action.Write], AssetsAcl.Scope.DataSet([2])),
        ],
        [AssetsAcl.Action.Read, AssetsAcl.Action.Write],
        None,
        id="AssetsAcl with multiple actions, no overlapping scope",
    )
    yield pytest.param(
        [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([123]))],
        [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
        None,
        id="One of multiple actions is missing capability",
    )


class TestTokenAPI:
    @pytest.mark.parametrize(
        "existing_capabilities,actions,expected_scope",
        list(get_scope_test_cases()),
    )
    def test_get_scope(
        self,
        existing_capabilities: list[Capability],
        actions: list[Capability.Action],
        expected_scope: list[Capability.Scope] | None,
    ):
        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = TokenInspection(
                subject="test_subject",
                projects=[ProjectSpec("https://example.com", groups=[123])],
                capabilities=ProjectCapabilityList(
                    [ProjectCapability(cap, ProjectsScope(["my_project"])) for cap in existing_capabilities]
                ),
            )

            actual_scope = client.token.get_scope(actions)
            assert actual_scope == expected_scope

    @pytest.mark.parametrize(
        "actions, error_message",
        [
            pytest.param([], "No actions provided to get_scope", id="Empty actions list"),
            pytest.param(
                [TimeSeriesAcl.Action.Read, AssetsAcl.Action.Write],
                "Actions belong to multiple ACL classes: AssetsAcl and TimeSeriesAcl.",
                id="Multiple actions with different ACL classes",
            ),
            pytest.param([object], "Unknown action class: type", id="Unknown action class"),
        ],
    )
    def test_get_scope_invalid_inputs(self, actions: list[Capability.Action], error_message: str) -> None:
        with monkeypatch_toolkit_client() as client:
            with pytest.raises(ValueError) as exc_info:
                client.token.get_scope(actions)

        assert str(exc_info.value) == error_message
