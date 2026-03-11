import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    Acl,
    AllScope,
    AssetsAcl,
    DataModelsAcl,
    DataSetScope,
    EventsAcl,
    GroupsAcl,
    Scope,
    SpaceIDScope,
)
from cognite_toolkit._cdf_tk.client.resource_classes.token import (
    AllProjects,
    InspectCapability,
    InspectProjectInfo,
    InspectResponse,
    ProjectCapabilities,
)


class TestProjectCapability:
    @pytest.mark.parametrize(
        "capabilities, required_acls, expected_missing",
        [
            pytest.param(
                {(AssetsAcl, "READ"): AllScope()},
                [EventsAcl(actions=["READ"], scope=DataSetScope(ids=[1]))],
                [EventsAcl(actions=["READ"], scope=DataSetScope(ids=[1]))],
                id="Missing EventsAcl with DataSetScope",
            ),
            pytest.param(
                {(AssetsAcl, "READ"): AllScope()},
                [],
                [],
                id="Empty required ACLs returns nothing missing",
            ),
            pytest.param(
                {(AssetsAcl, "READ"): AllScope()},
                [AssetsAcl(actions=["READ"], scope=AllScope())],
                [],
                id="Exact match on ACL type and action",
            ),
            pytest.param(
                {(AssetsAcl, "READ"): AllScope()},
                [AssetsAcl(actions=["READ"], scope=DataSetScope(ids=[42]))],
                [],
                id="Matching type and action with all scope satisfies specific scope",
            ),
            pytest.param(
                {(AssetsAcl, "READ"): DataSetScope(ids=[42])},
                [AssetsAcl(actions=["READ"], scope=AllScope())],
                [AssetsAcl(actions=["READ"], scope=AllScope())],
                id="Matching type and action but missing scope",
            ),
            pytest.param(
                {(AssetsAcl, "READ"): AllScope()},
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
                {(DataModelsAcl, "READ"): SpaceIDScope(space_ids=["my_space"])},
                [DataModelsAcl(actions=["READ"], scope=SpaceIDScope(space_ids=["other_space"]))],
                [DataModelsAcl(actions=["READ"], scope=SpaceIDScope(space_ids=["other_space"]))],
                id="SpaceIDScope ACL present regardless of scope content",
            ),
            pytest.param(
                {(GroupsAcl, "READ"): AllScope(), (GroupsAcl, "LIST"): AllScope()},
                [GroupsAcl(actions=["READ", "LIST", "CREATE"], scope=AllScope())],
                [GroupsAcl(actions=["CREATE"], scope=AllScope())],
                id="Three actions with one missing reports all actions",
            ),
        ],
    )
    def test_verify(
        self,
        capabilities: dict[tuple[type[Acl], str], Scope],
        required_acls: list[Acl],
        expected_missing: list[Acl],
    ) -> None:
        project = ProjectCapabilities(capabilities=capabilities, name="MyProject", groups=[37])
        actual = project.verify(required_acls)

        assert actual == expected_missing

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
                ProjectCapabilities({(AssetsAcl, "READ"): AllScope()}, name="test_project", groups=[]),
                id="Union of scopes with same action should result in the most permissive scope (AllScope in this case)",
            )
        ],
    )
    def test_to_project_capabilities(self, token: InspectResponse, expected_capabilities: ProjectCapabilities) -> None:
        assert token.to_project_capabilities() == expected_capabilities
