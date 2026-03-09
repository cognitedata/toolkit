import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.group import Acl, AllScope, AssetsAcl, DataSetScope, EventsAcl
from cognite_toolkit._cdf_tk.client.resource_classes.token import ProjectCapabilities


class TestProjectCapability:
    @pytest.mark.parametrize(
        "required_acls, expected_missing",
        [
            pytest.param(
                [EventsAcl(actions=["READ"], scope=DataSetScope(ids=[1]))],
                [EventsAcl(actions=["READ"], scope=DataSetScope(ids=[1]))],
                id="Missing EventsAcl with DataSetScope",
            )
        ],
    )
    def test_verify(self, required_acls: list[Acl], expected_missing: list[Acl]) -> None:
        project = ProjectCapabilities(capabilities={(AssetsAcl, "READ"): AllScope()}, name="MyProject", groups=[37])
        actual = project.verify(required_acls)

        assert actual == expected_missing
