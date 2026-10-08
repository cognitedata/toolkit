from collections.abc import Sequence

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    Acl,
    AllScope,
    AssetsAcl,
    CurrentUserScope,
    DataModelsAcl,
    DataSetScope,
    EventsAcl,
    GroupsAcl,
    IDScopeLowerCase,
    RawAcl,
    Scope,
    SpaceIDScope,
    TableScope,
    TimeSeriesAcl,
    UnknownAcl,
    UnknownScope,
)
from cognite_toolkit._cdf_tk.client.resource_classes.token import (
    AclAction,
    AclName,
    AllProjects,
    FlatCapabilities,
    InspectCapability,
    InspectProjectInfo,
    InspectResponse,
)


class TestProjectCapability:
    @pytest.mark.parametrize(
        "capabilities, required_acls, expected_missing",
        [
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [AllScope()]},
                [EventsAcl(actions=["READ"], scope=DataSetScope(ids=[1]))],
                [EventsAcl(actions=["READ"], scope=DataSetScope(ids=[1]))],
                id="Missing EventsAcl with DataSetScope",
            ),
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [AllScope()]},
                [],
                [],
                id="Empty required ACLs returns nothing missing",
            ),
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [AllScope()]},
                [AssetsAcl(actions=["READ"], scope=AllScope())],
                [],
                id="Exact match on ACL type and action",
            ),
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [AllScope()]},
                [AssetsAcl(actions=["READ"], scope=DataSetScope(ids=[42]))],
                [],
                id="Matching type and action with all scope satisfies specific scope",
            ),
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [DataSetScope(ids=[42])]},
                [AssetsAcl(actions=["READ"], scope=AllScope())],
                [AssetsAcl(actions=["READ"], scope=AllScope())],
                id="Matching type and action but missing scope",
            ),
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [AllScope()]},
                [AssetsAcl(actions=["WRITE"], scope=AllScope())],
                [AssetsAcl(actions=["WRITE"], scope=AllScope())],
                id="Same ACL type but missing action",
            ),
            pytest.param(
                {},
                [
                    AssetsAcl(actions=["READ"], scope=AllScope()),
                    EventsAcl(actions=["WRITE"], scope=DataSetScope(ids=[1])),
                ],
                [
                    AssetsAcl(actions=["READ"], scope=AllScope()),
                    EventsAcl(actions=["WRITE"], scope=DataSetScope(ids=[1])),
                ],
                id="Empty capabilities means all ACLs missing",
            ),
            pytest.param(
                {(DataModelsAcl, "dataModelsAcl", "READ"): [SpaceIDScope(space_ids=["my_space"])]},
                [DataModelsAcl(actions=["READ"], scope=SpaceIDScope(space_ids=["other_space"]))],
                [DataModelsAcl(actions=["READ"], scope=SpaceIDScope(space_ids=["other_space"]))],
                id="SpaceIDScope ACL present regardless of scope content",
            ),
            pytest.param(
                {(GroupsAcl, "groupsAcl", "READ"): [AllScope()], (GroupsAcl, "groupsAcl", "LIST"): [AllScope()]},
                [GroupsAcl(actions=["READ", "LIST", "CREATE"], scope=AllScope())],
                [GroupsAcl(actions=["CREATE"], scope=AllScope())],
                id="Three actions with one missing reports all actions",
            ),
        ],
    )
    def test_verify(
        self,
        capabilities: dict[tuple[type[Acl], AclName, AclAction], list[Scope]],
        required_acls: list[Acl],
        expected_missing: list[Acl],
    ) -> None:
        project = FlatCapabilities(capabilities=capabilities, name="MyProject", groups=[37])
        actual = project.verify(required_acls)

        assert actual == expected_missing

    @pytest.mark.parametrize(
        "capabilities, acl_cls, actions, expected_scopes",
        [
            pytest.param(
                {(AssetsAcl, "assetsAcl", "READ"): [AllScope()]},
                AssetsAcl,
                ["READ"],
                [AllScope()],
                id="Exact match on ACL type and action with AllScope",
            ),
            pytest.param(
                {
                    (AssetsAcl, "assetsAcl", "READ"): [DataSetScope(ids=[37, 42])],
                    (AssetsAcl, "assetsAcl", "WRITE"): [DataSetScope(ids=[37])],
                },
                AssetsAcl,
                ["READ", "WRITE"],
                [DataSetScope(ids=[37])],
                id="Intersection of scopes for multiple actions of the same ACL type",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[37, 42]), IDScopeLowerCase(ids=[37])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [DataSetScope(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [DataSetScope(ids=[37])],
                id="Intersection of scopes for multiple actions of the same ACL type with different scope types",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[42])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [DataSetScope(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [],
                id="No intersection of scopes for multiple actions of the same ACL type",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [DataSetScope(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ"],
                [],
                id="No scopes available for the requested action of the ACL type",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [AllScope(), DataSetScope(ids=[37])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [AllScope()],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [AllScope()],
                id="AllScope for one action results in AllScope for the intersection of scopes for multiple actions of the same ACL type",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [AllScope(), DataSetScope(ids=[37])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [DataSetScope(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [DataSetScope(ids=[37])],
                id="AllScope in first action together with a narrower scope of the same type as second action",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[37])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [AllScope(), DataSetScope(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [DataSetScope(ids=[37])],
                id="AllScope in second action together with a narrower scope of the same type as first action",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [AllScope()],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [AllScope(), DataSetScope(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [AllScope()],
                id="AllScope in both actions, second action also has a narrower scope (order-swapped existing case)",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[37, 42])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [AllScope()],
                    (TimeSeriesAcl, "timeSeriesAcl", "LIST"): [DataSetScope(ids=[42, 99])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE", "LIST"],
                [DataSetScope(ids=[42])],
                id="Three actions with AllScope in the middle",
            ),
            pytest.param(
                {
                    (TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[37])],
                    (TimeSeriesAcl, "timeSeriesAcl", "WRITE"): [IDScopeLowerCase(ids=[37])],
                },
                TimeSeriesAcl,
                ["READ", "WRITE"],
                [],
                id="Different scope types for each action have no intersection",
            ),
            pytest.param(
                {(TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[37])]},
                TimeSeriesAcl,
                ["READ", "READ"],
                [DataSetScope(ids=[37])],
                id="Duplicate actions",
            ),
            pytest.param(
                {
                    (GroupsAcl, "groupsAcl", "READ"): [CurrentUserScope()],
                    (GroupsAcl, "groupsAcl", "LIST"): [CurrentUserScope()],
                },
                GroupsAcl,
                ["READ", "LIST"],
                [CurrentUserScope()],
                id="Scope without data fields (CurrentUserScope)",
            ),
            pytest.param(
                {
                    (RawAcl, "rawAcl", "READ"): [TableScope(dbs_to_tables={"db1": ["t1", "t2"], "db2": ["t3"]})],
                    (RawAcl, "rawAcl", "WRITE"): [TableScope(dbs_to_tables={"db1": ["t2", "t4"]})],
                },
                RawAcl,
                ["READ", "WRITE"],
                [TableScope(dbs_to_tables={"db1": ["t2"]})],
                id="TableScope intersection",
            ),
            pytest.param(
                {
                    (RawAcl, "rawAcl", "READ"): [TableScope(dbs_to_tables={"db1": []})],
                    (RawAcl, "rawAcl", "WRITE"): [TableScope(dbs_to_tables={"db1": ["t1"]})],
                },
                RawAcl,
                ["READ", "WRITE"],
                [TableScope(dbs_to_tables={"db1": []})],
                id="TableScope with empty table list (entire database) intersected with specific table",
            ),
            pytest.param(
                {
                    (AssetsAcl, "assetsAcl", "READ"): [
                        UnknownScope.model_validate({"scopeName": "newScope", "someIds": [1]})
                    ],
                },
                AssetsAcl,
                ["READ"],
                [UnknownScope.model_validate({"scopeName": "newScope", "someIds": [1]})],
                id="Single action with unknown scope",
            ),
            pytest.param(
                {
                    (AssetsAcl, "assetsAcl", "READ"): [
                        AllScope(),
                        UnknownScope.model_validate({"scopeName": "newScope", "someIds": [1]}),
                    ],
                    (AssetsAcl, "assetsAcl", "WRITE"): [AllScope()],
                },
                AssetsAcl,
                ["READ", "WRITE"],
                [AllScope()],
                id="AllScope in both actions with an unknown scope in one of them",
            ),
        ],
    )
    def test_get_available_scopes(
        self,
        capabilities: dict[tuple[type[Acl], AclName, AclAction], list[Scope]],
        acl_cls: type[Acl],
        actions: Sequence[str],
        expected_scopes: list[Scope],
    ) -> None:
        project = FlatCapabilities(capabilities=capabilities, name="MyProject", groups=[37])
        available_scopes = project.get_available_scopes(acl_cls, actions)

        assert available_scopes == expected_scopes

    @pytest.mark.parametrize(
        "token, expected_capabilities",
        [
            pytest.param(
                InspectResponse(
                    subject="test",
                    projects=[InspectProjectInfo(project_url_name="test_project", groups=[])],
                    project="test_project",
                    capabilities=[
                        InspectCapability(
                            acl=AssetsAcl(actions=["READ"], scope=DataSetScope(ids=[1])),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=AssetsAcl(actions=["READ"], scope=AllScope()),
                            project_scope=AllProjects(all_projects={}),
                        ),
                    ],
                ),
                FlatCapabilities({(AssetsAcl, "assetsAcl", "READ"): [AllScope()]}, name="test_project", groups=[]),
                id="Union of scopes with same action should result in the most permissive scope (AllScope in this case)",
            ),
            pytest.param(
                InspectResponse(
                    subject="test",
                    projects=[InspectProjectInfo(project_url_name="test_project", groups=[])],
                    project="test_project",
                    capabilities=[
                        InspectCapability(
                            acl=UnknownAcl(
                                actions=["READ"],
                                scope=UnknownScope.model_validate({"scopeName": "unknown_scope", "someIds": [1, 2]}),
                                acl_name="unknown_acl",
                            ),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=UnknownAcl(
                                actions=["READ"],
                                scope=UnknownScope.model_validate({"scopeName": "unknown_scope", "someIds": [2, 3]}),
                                acl_name="unknown_acl",
                            ),
                            project_scope=AllProjects(all_projects={}),
                        ),
                    ],
                ),
                FlatCapabilities(
                    {
                        (UnknownAcl, "unknown_acl", "READ"): [
                            UnknownScope.model_validate({"scopeName": "unknown_scope", "someIds": [1, 2, 3]})
                        ],
                    },
                    name="test_project",
                    groups=[],
                ),
                id="Unknown ACL types should be included in the capabilities with their scopes intact",
            ),
            pytest.param(
                InspectResponse(
                    subject="test",
                    projects=[InspectProjectInfo(project_url_name="test_project", groups=[])],
                    project="test_project",
                    capabilities=[
                        InspectCapability(
                            acl=TimeSeriesAcl(
                                actions=["READ"],
                                scope=DataSetScope(ids=[1]),
                            ),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(actions=["READ"], scope=IDScopeLowerCase(ids=[2, 3])),
                            project_scope=AllProjects(all_projects={}),
                        ),
                    ],
                ),
                FlatCapabilities(
                    {
                        (TimeSeriesAcl, "timeSeriesAcl", "READ"): [DataSetScope(ids=[1]), IDScopeLowerCase(ids=[2, 3])],
                    },
                    name="test_project",
                    groups=[],
                ),
                id="Multiple ACLs of the same type with different scopes should be included in the capabilities with their respective scopes intact",
            ),
            pytest.param(
                InspectResponse(
                    subject="test",
                    projects=[InspectProjectInfo(project_url_name="test_project", groups=[])],
                    project="test_project",
                    capabilities=[
                        InspectCapability(
                            acl=TimeSeriesAcl(
                                actions=["READ"],
                                scope=DataSetScope(ids=[1]),
                            ),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(actions=["READ"], scope=IDScopeLowerCase(ids=[2, 3])),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(actions=["READ"], scope=AllScope()),
                            project_scope=AllProjects(all_projects={}),
                        ),
                    ],
                ),
                FlatCapabilities(
                    {
                        (TimeSeriesAcl, "timeSeriesAcl", "READ"): [AllScope()],
                    },
                    name="test_project",
                    groups=[],
                ),
                id="If any ACL of a given type and action has AllScope, the resulting capabilities should only include AllScope for that type and action",
            ),
            pytest.param(
                InspectResponse(
                    subject="test",
                    projects=[InspectProjectInfo(project_url_name="test_project", groups=[])],
                    project="test_project",
                    capabilities=[
                        InspectCapability(
                            acl=TimeSeriesAcl(
                                actions=["READ"],
                                scope=DataSetScope(ids=[1]),
                            ),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(
                                actions=["READ"],
                                scope=DataSetScope(ids=[2]),
                            ),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(actions=["READ"], scope=IDScopeLowerCase(ids=[2])),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(actions=["READ"], scope=IDScopeLowerCase(ids=[3])),
                            project_scope=AllProjects(all_projects={}),
                        ),
                        InspectCapability(
                            acl=TimeSeriesAcl(actions=["READ"], scope=IDScopeLowerCase(ids=[3])),
                            project_scope=AllProjects(all_projects={}),
                        ),
                    ],
                ),
                FlatCapabilities(
                    {
                        (TimeSeriesAcl, "timeSeriesAcl", "READ"): [
                            DataSetScope(ids=[1, 2]),
                            IDScopeLowerCase(ids=[2, 3]),
                        ],
                    },
                    name="test_project",
                    groups=[],
                ),
                id="Multiple ACLs of the same type and action with overlapping scopes should be merged into a single scope for each unique scope type",
            ),
        ],
    )
    def test_to_project_capabilities(self, token: InspectResponse, expected_capabilities: FlatCapabilities) -> None:
        assert token.to_project_capabilities() == expected_capabilities
