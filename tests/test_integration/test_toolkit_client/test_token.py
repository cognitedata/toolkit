import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, AssetsAcl, DataSetScope
from cognite_toolkit._cdf_tk.client.resource_classes.token import (
    AllProjects,
    InspectCapability,
    InspectProjectInfo,
    InspectResponse,
    ProjectCapabilities,
)


class TestTokenAPI:
    def test_inspect(self, toolkit_client: ToolkitClient) -> None:
        token = toolkit_client.tool.token.inspect()
        token.to_project_capabilities()
        assert isinstance(token, InspectResponse)

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
