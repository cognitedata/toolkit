from graphlib import CycleError

import pytest
from cognite.client.data_classes import ClientCredentials, OidcCredentials
from cognite.client.data_classes.data_modeling import ContainerId, MappedPropertyApply, ViewApply, ViewId

from cognite_toolkit._cdf_tk.utils.cdf import append_parent_properties, parents_by_child_views, try_find_error


class TestTryFindError:
    @pytest.mark.parametrize(
        "credentials, expected",
        [
            pytest.param(
                ClientCredentials("${ENVIRONMENT_VAR}", "1234"),
                "The environment variable is not set: ENVIRONMENT_VAR.",
                id="Missing environment variable",
            ),
            pytest.param(
                ClientCredentials("${ENV1}", "${ENV2}"),
                "The environment variables are not set: ENV1 and ENV2.",
                id="Missing environment variable",
            ),
            pytest.param(
                OidcCredentials("my-client-id", "123", ["https://cognite.com"], "not-valid-uri", "my-project"),
                "The tokenUri 'not-valid-uri' is not a valid URI.",
            ),
            pytest.param(None, None, id="empty"),
        ],
    )
    def test_try_find_error(self, credentials: OidcCredentials | ClientCredentials | None, expected: str | None):
        assert try_find_error(credentials) == expected


@pytest.fixture()
def child_parent_grand_parent() -> list[ViewApply]:
    grand_parent = ViewApply(
        space="my_space",
        external_id="GrandParent",
        version="v1",
        properties={
            "grandParentProp": MappedPropertyApply(
                ContainerId("my_space", "container"),
                "grand_parent_prop",
            ),
            "overwritten": MappedPropertyApply(
                ContainerId("my_space", "container"),
                "overwritten",
                source=ViewId("my_space", "GrandParentTarget", "v1"),
            ),
        },
    )
    parent = ViewApply(
        space="my_space",
        external_id="Parent",
        version="v1",
        properties={
            "parentProp": MappedPropertyApply(
                ContainerId("my_space", "container"),
                "parentProp",
            )
        },
        implements=[grand_parent.as_id()],
    )
    child = ViewApply(
        space="my_space",
        external_id="Child",
        version="v1",
        properties={
            "childProp": MappedPropertyApply(
                ContainerId("my_space", "container"),
                "childProp",
            ),
            "overwritten": MappedPropertyApply(
                ContainerId("my_space", "container"), "overwritten", source=ViewId("my_space", "ChildTarget", "v1")
            ),
        },
        implements=[parent.as_id()],
    )
    return [child, parent, grand_parent]


class TestAddParentProperties:
    def test_append_parent_properties(self, child_parent_grand_parent: list[ViewApply]) -> None:
        append_parent_properties(child_parent_grand_parent)
        child, parent, grand_parent = child_parent_grand_parent
        assert set(child.properties.keys()) == {"childProp", "parentProp", "grandParentProp", "overwritten"}
        overwritten = child.properties["overwritten"]
        assert isinstance(overwritten, MappedPropertyApply)
        assert overwritten.source == ViewId("my_space", "ChildTarget", "v1")
        assert set(parent.properties.keys()) == {"parentProp", "grandParentProp", "overwritten"}
        overwritten = parent.properties["overwritten"]
        assert isinstance(overwritten, MappedPropertyApply)
        assert overwritten.source == ViewId("my_space", "GrandParentTarget", "v1")
        assert set(grand_parent.properties.keys()) == {"grandParentProp", "overwritten"}
        overwritten = grand_parent.properties["overwritten"]
        assert isinstance(overwritten, MappedPropertyApply)
        assert overwritten.source == ViewId("my_space", "GrandParentTarget", "v1")

    def test_append_parent_properties_cyclic_dependency(self) -> None:
        view1 = ViewApply("my_space", "view1", "v1", implements=[ViewId("my_space", "view2", "v1")])
        view2 = ViewApply("my_space", "view2", "v1", implements=[ViewId("my_space", "view1", "v1")])
        with pytest.raises(CycleError) as e:
            append_parent_properties([view1, view2])

        assert str(e.value.args[0]) == "nodes are in a cycle"


class TestParentsByChildViews:
    def test_parents_by_child_views(self, child_parent_grand_parent: list[ViewApply]) -> None:
        parents_by_child = parents_by_child_views(child_parent_grand_parent)
        assert parents_by_child == {
            ViewId("my_space", "Child", "v1"): {
                ViewId("my_space", "Parent", "v1"),
                ViewId("my_space", "GrandParent", "v1"),
            },
            ViewId("my_space", "Parent", "v1"): {ViewId("my_space", "GrandParent", "v1")},
            ViewId("my_space", "GrandParent", "v1"): set(),
        }
