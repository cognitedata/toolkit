from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ConstraintOrIndexState,
    ContainerId,
    ContainerPropertyDefinition,
    ContainerResponse,
    DirectNodeRelation,
    RequiresConstraintDefinition,
    SpaceId,
    TextProperty,
    ViewCorePropertyResponse,
    ViewId,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._data_model import DataModelResponseWithViews
from cognite_toolkit._cdf_tk.cruds import (
    ContainerCRUD,
    ResourceIO,
    ResourceWorker,
    SpaceCRUD,
    ViewIO,
)
from cognite_toolkit._cdf_tk.yaml_classes.containers import ContainerYAML
from cognite_toolkit._cdf_tk.yaml_classes.views import ViewYAML
from tests.test_unit.approval_client import ApprovalToolkitClient


def _create_test_container(
    external_id: str,
    properties: dict[str, ContainerPropertyDefinition],
    constraints: dict | None = None,
    space: str = "my_space",
) -> ContainerResponse:
    """Helper to create a Container with standard test defaults."""
    return ContainerResponse(
        space=space,
        external_id=external_id,
        properties=properties,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        used_for="node",
        constraints=constraints or {},
        description=None,
        name=None,
        indexes={},
    )


def _create_test_view(external_id: str, container: ContainerResponse) -> ViewResponse:
    """Helper to create a View that maps to all properties of the given Container."""
    properties = {}
    for prop_name, container_prop in container.properties.items():
        properties[prop_name] = ViewCorePropertyResponse(
            container=container.as_id(),
            container_property_identifier=prop_name,
            type=container_prop.type,
            nullable=container_prop.nullable,
            immutable=container_prop.immutable,
            auto_increment=container_prop.auto_increment,
            constraint_state=ConstraintOrIndexState(),
        )

    return ViewResponse(
        space=container.space,
        external_id=external_id,
        version="v1",
        properties=properties,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        used_for="node",
        writable=True,
        queryable=True,
        description=None,
        name=None,
        filter=None,
        implements=None,
        mapped_containers=[container.as_id()],
    )


class TestViewLoader:
    def test_unchanged_view_int_version(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ViewIO.create_loader(toolkit_client_approval.mock_client)
        raw_file = """- space: sp_space
  externalId: my_view
  version: 1"""
        file = MagicMock(spec=Path)
        file.read_text.return_value = raw_file
        cdf_view = ViewResponse(
            space="sp_space",
            external_id="my_view",
            version="1",
            last_updated_time=1,
            created_time=1,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={},
            mapped_containers=[],
            queryable=True,
        )

        toolkit_client_approval.append(ViewResponse, [cdf_view])

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([file])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "space": "sp_my_space",
                    "properties": {
                        "name": {
                            "container": {
                                "type": "container",
                                "space": "my_container_space",
                                "externalId": "my_container",
                            }
                        }
                    },
                },
                [
                    (SpaceCRUD, SpaceId(space="sp_my_space")),
                    (ContainerCRUD, ContainerId(space="my_container_space", external_id="my_container")),
                ],
                id="View with one container property",
            ),
            pytest.param(
                {
                    "space": "sp_my_space",
                    "properties": {
                        "toEdge": {
                            "source": {
                                "type": "view",
                                "space": "my_view_space",
                                "externalId": "my_view",
                                "version": "1",
                            },
                            "edgeSource": {
                                "type": "view",
                                "space": "my_other_view_space",
                                "externalId": "my_edge_view",
                                "version": "42",
                            },
                        }
                    },
                },
                [
                    (SpaceCRUD, SpaceId(space="sp_my_space")),
                    (ViewIO, ViewId(space="my_view_space", external_id="my_view", version="1")),
                    (ViewIO, ViewId(space="my_other_view_space", external_id="my_edge_view", version="42")),
                ],
                id="View with one container property",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceIO], Hashable]]) -> None:
        actual = ViewIO.get_dependent_items(item)

        assert list(actual) == expected

    @pytest.mark.parametrize(
        "view_ids,ordering_constraints,test_description",
        [
            pytest.param(
                [
                    ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                ],
                [
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ),
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ),
                ],
                "Transitive chain: CogniteSourceSystem -> CogniteSourceable -> CogniteAsset",
                id="transitive_chain",
            ),
            pytest.param(
                [
                    ViewId(space="cdf_cdm", external_id="CogniteActivity", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteSchedulable", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                ],
                [
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ),
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteSchedulable", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteActivity", version="v1"),
                    ),
                ],
                "Multiple independent chains: (CogniteSourceSystem -> CogniteSourceable) and (CogniteSchedulable -> CogniteActivity)",
                id="multiple_independent_chains",
            ),
            pytest.param(
                [
                    ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteAssetClass", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteAssetType", version="v1"),
                    ViewId(space="cdf_cdm", external_id="CogniteDescribable", version="v1"),
                ],
                [
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteDescribable", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteAssetType", version="v1"),
                    ),
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteDescribable", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteAssetClass", version="v1"),
                    ),
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteAssetClass", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ),
                    (
                        ViewId(space="cdf_cdm", external_id="CogniteAssetType", version="v1"),
                        ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ),
                ],
                "Diamond: CogniteDescribable -> CogniteAssetType/CogniteAssetClass -> CogniteAsset",
                id="diamond_dependency",
            ),
        ],
    )
    def test_topological_sort_container_constraints_dependency_patterns(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cognite_core_no_3D: DataModelResponseWithViews,
        cognite_core_containers_no_3D: list[ContainerResponse],
        view_ids: list[ViewId],
        ordering_constraints: list[tuple[ViewId, ViewId]],
        test_description: str,
    ) -> None:
        """Test various dependency patterns: transitive chains, independent chains, and diamond dependencies."""
        loader = ViewIO.create_loader(toolkit_client_approval.mock_client)
        toolkit_client_approval.append(ViewResponse, cognite_core_no_3D.views)
        toolkit_client_approval.append(ContainerResponse, cognite_core_containers_no_3D)

        sorted_views, cyclic_views = loader.topological_sort_container_constraints(view_ids)

        assert not cyclic_views, f"{test_description}: Expected no cyclic views, got {cyclic_views}"

        # Verify same number of views returned
        assert len(sorted_views) == len(view_ids), (
            f"{test_description}: Expected {len(view_ids)} views, got {len(sorted_views)}"
        )

        # Verify all ordering constraints
        for before_view_id, after_view_id in ordering_constraints:
            before_idx = sorted_views.index(before_view_id)
            after_idx = sorted_views.index(after_view_id)
            assert before_idx < after_idx, (
                f"{test_description}: {before_view_id.external_id} should come before {after_view_id.external_id}"
            )

    def test_topological_sort_container_constraints_cyclical_dependency(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test that views with cyclical dependencies are returned separately from the sorted views."""

        loader = ViewIO.create_loader(toolkit_client_approval.mock_client)

        # Create three containers that form a cycle: A -> B -> C -> A
        container_a = _create_test_container(
            "ContainerA",
            {
                "refB": ContainerPropertyDefinition(
                    type=DirectNodeRelation(container=ContainerId(space="my_space", external_id="ContainerB")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )
        container_b = _create_test_container(
            "ContainerB",
            {
                "name": ContainerPropertyDefinition(
                    type=TextProperty(), nullable=True, immutable=False, auto_increment=False
                ),
            },
            {
                "requiresA": RequiresConstraintDefinition(
                    require=ContainerId(space="my_space", external_id="ContainerA")
                ),
            },
        )
        container_c = _create_test_container(
            "ContainerC",
            {
                "name": ContainerPropertyDefinition(
                    type=TextProperty(), nullable=True, immutable=False, auto_increment=False
                ),
            },
        )
        view_a = _create_test_view("ViewA", container_a)
        view_b = _create_test_view("ViewB", container_b)
        view_c = _create_test_view("ViewC", container_c)

        toolkit_client_approval.append(ViewResponse, [view_a, view_b, view_c])
        toolkit_client_approval.append(ContainerResponse, [container_a, container_b, container_c])

        view_ids = [view_a.as_id(), view_b.as_id(), view_c.as_id()]

        sorted_views, cyclic_views = loader.topological_sort_container_constraints(view_ids)

        assert sorted_views == [view_c.as_id()]
        assert set(cyclic_views) == {view_a.as_id(), view_b.as_id()}

    @pytest.mark.parametrize(
        "view_id,expected_readonly_props",
        [
            pytest.param(
                ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                {"pathLastUpdatedTime", "path", "root"},
                id="CogniteAsset_has_readonly_properties",
            ),
            pytest.param(
                ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1"),
                {"isUploaded", "uploadedTime"},
                id="CogniteFile_has_readonly_properties",
            ),
            pytest.param(
                ViewId(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                set(),
                id="CogniteSourceSystem_no_readonly_properties",
            ),
        ],
    )
    def test_get_readonly_properties(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cognite_core_no_3D: DataModelResponseWithViews,
        view_id: ViewId,
        expected_readonly_props: set[str],
    ) -> None:
        """Test that get_readonly_properties identifies readonly properties from containers."""
        loader = ViewIO.create_loader(toolkit_client_approval.mock_client)
        toolkit_client_approval.append(ViewResponse, cognite_core_no_3D.views)

        readonly_props = loader.get_readonly_properties(view_id)
        assert readonly_props == expected_readonly_props


class TestContainerCRUDGetDependencies:
    """Test get_dependencies method for ContainerCRUD."""

    def test_container_with_space_only(self) -> None:
        """Test Container with no property dependencies."""
        container = ContainerYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_container",
                "description": "A simple container",
                "properties": {
                    "name": {"type": {"type": "text"}},
                },
            }
        )

        deps = list(ContainerCRUD.get_dependencies(container))
        assert len(deps) == 1
        assert deps[0] == (SpaceCRUD, SpaceId(space="my_space"))

    def test_container_with_direct_node_relation(self) -> None:
        """Test Container with DirectNodeRelation dependency."""
        container = ContainerYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_container",
                "properties": {
                    "relatedNode": {
                        "type": {
                            "type": "direct",
                            "container": {"type": "container", "space": "other_space", "externalId": "other_container"},
                        }
                    }
                },
            }
        )

        deps = list(ContainerCRUD.get_dependencies(container))
        assert len(deps) == 2
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ContainerCRUD, ContainerId(space="other_space", external_id="other_container")) in deps

    def test_container_with_requires_constraint(self) -> None:
        """Test Container with RequiresConstraintDefinition dependency."""
        container = ContainerYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_container",
                "properties": {"name": {"type": {"type": "text"}}},
                "constraints": {
                    "requires_constraint": {
                        "constraintType": "requires",
                        "require": {"type": "container", "space": "other_space", "externalId": "required_container"},
                    }
                },
            }
        )

        deps = list(ContainerCRUD.get_dependencies(container))
        assert len(deps) == 2
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ContainerCRUD, ContainerId(space="other_space", external_id="required_container")) in deps


class TestViewCRUDGetDependencies:
    """Test get_dependencies method for ViewCRUD."""

    def test_view_with_space_only(self) -> None:
        """Test View with no dependencies."""
        view = ViewYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_view",
                "version": "1",
                "description": "A simple view",
            }
        )

        deps = list(ViewIO.get_dependencies(view))
        assert len(deps) == 1
        assert deps[0] == (SpaceCRUD, SpaceId(space="my_space"))

    def test_view_with_container_property(self) -> None:
        """Test View with ContainerViewProperty dependency."""
        view = ViewYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_view",
                "version": "1",
                "properties": {
                    "name": {
                        "container": {"type": "container", "space": "container_space", "externalId": "my_container"},
                        "containerPropertyIdentifier": "name",
                    }
                },
            }
        )

        deps = list(ViewIO.get_dependencies(view))
        assert len(deps) == 2
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ContainerCRUD, ContainerId(space="container_space", external_id="my_container")) in deps

    def test_view_with_implements(self) -> None:
        """Test View with implements dependency."""
        view = ViewYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "derived_view",
                "version": "1",
                "implements": [{"type": "view", "space": "my_space", "externalId": "base_view", "version": "1"}],
            }
        )

        deps = list(ViewIO.get_dependencies(view))
        assert len(deps) == 2
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ViewIO, ViewId(space="my_space", external_id="base_view", version="1")) in deps

    def test_view_with_edge_connection(self) -> None:
        """Test View with EdgeConnectionDefinition dependency."""
        view = ViewYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "edge_view",
                "version": "1",
                "properties": {
                    "connectedTo": {
                        "connectionType": "single_edge_connection",
                        "source": {
                            "type": "view",
                            "space": "source_space",
                            "externalId": "source_view",
                            "version": "1",
                        },
                        "type": {"space": "edge_space", "externalId": "edge_type"},
                        "direction": "outwards",
                    }
                },
            }
        )

        deps = list(ViewIO.get_dependencies(view))
        assert len(deps) == 2
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ViewIO, ViewId(space="source_space", external_id="source_view", version="1")) in deps

    def test_view_with_reverse_direct_relation_view_through(self) -> None:
        """Test View with ReverseDirectRelationConnectionDefinition with View through reference."""
        view = ViewYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "reverse_view",
                "version": "1",
                "properties": {
                    "reverseConnection": {
                        "connectionType": "single_reverse_direct_relation",
                        "source": {
                            "type": "view",
                            "space": "source_space",
                            "externalId": "source_view",
                            "version": "1",
                        },
                        "through": {
                            "source": {
                                "type": "view",
                                "space": "through_space",
                                "externalId": "through_view",
                                "version": "1",
                            },
                            "identifier": "connectedTo",
                        },
                    }
                },
            }
        )

        deps = list(ViewIO.get_dependencies(view))
        assert len(deps) == 3
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ViewIO, ViewId(space="source_space", external_id="source_view", version="1")) in deps
        assert (ViewIO, ViewId(space="through_space", external_id="through_view", version="1")) in deps
