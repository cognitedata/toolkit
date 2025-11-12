from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, ResourceCRUD, ResourceWorker, SpaceCRUD, ViewCRUD
from tests.test_unit.approval_client import ApprovalToolkitClient


def _create_test_view(external_id: str, properties: dict) -> dm.View:
    """Helper to create a View with standard test defaults."""
    return dm.View(
        space="my_space",
        external_id=external_id,
        version="v1",
        properties=properties,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        used_for="node",
        writable=True,
        description=None,
        name=None,
        filter=None,
        implements=None,
    )


class TestViewLoader:
    def test_unchanged_view_int_version(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)
        raw_file = """- space: sp_space
  externalId: my_view
  version: 1"""
        file = MagicMock(spec=Path)
        file.read_text.return_value = raw_file
        cdf_view = dm.View(
            space="sp_space",
            external_id="my_view",
            version="1",
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={},
        )

        toolkit_client_approval.append(dm.View, [cdf_view])

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
                    (SpaceCRUD, "sp_my_space"),
                    (ContainerCRUD, dm.ContainerId(space="my_container_space", external_id="my_container")),
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
                    (SpaceCRUD, "sp_my_space"),
                    (ViewCRUD, dm.ViewId(space="my_view_space", external_id="my_view", version="1")),
                    (ViewCRUD, dm.ViewId(space="my_other_view_space", external_id="my_edge_view", version="42")),
                ],
                id="View with one container property",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = ViewCRUD.get_dependent_items(item)

        assert list(actual) == expected

    @pytest.mark.parametrize(
        "constraint_type,container_b_properties,container_b_constraints",
        [
            pytest.param(
                "DirectRelation",
                {
                    "parent": dm.ContainerProperty(
                        type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerA")),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    )
                },
                {},
                id="direct_relation",
            ),
            pytest.param(
                "RequiresConstraint",
                {"name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)},
                {"requiresA": dm.RequiresConstraint(require=dm.ContainerId("my_space", "ContainerA"))},
                id="requires_constraint",
            ),
        ],
    )
    def test_topological_sort_container_constraints_with_dependencies(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        constraint_type: str,
        container_b_properties: dict,
        container_b_constraints: dict,
    ) -> None:
        """Test sorting views by container dependencies (DirectRelation or RequiresConstraint)."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Container A has no dependencies
        container_a = dm.Container(
            space="my_space",
            external_id="ContainerA",
            properties={
                "name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Container B depends on Container A (via DirectRelation or RequiresConstraint)
        container_b = dm.Container(
            space="my_space",
            external_id="ContainerB",
            properties=container_b_properties,
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints=container_b_constraints,
            description=None,
            name=None,
            indexes={},
        )

        # View A maps to Container A
        view_a = _create_test_view(
            external_id="ViewA",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerA"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        # View B maps to Container B (which depends on Container A)
        view_b = _create_test_view(
            external_id="ViewB",
            properties={
                list(container_b_properties.keys())[0]: dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerB"),
                    container_property_identifier=list(container_b_properties.keys())[0],
                    type=dm.DirectRelation() if constraint_type == "DirectRelation" else dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        toolkit_client_approval.append(dm.View, [view_a, view_b])
        toolkit_client_approval.append(dm.Container, [container_a, container_b])

        # ViewB depends on ViewA because ContainerB depends on ContainerA
        view_ids = [view_b.as_id(), view_a.as_id()]
        sorted_views = loader.topological_sort_container_constraints(view_ids)

        # ViewA should come before ViewB
        assert sorted_views.index(view_a.as_id()) < sorted_views.index(view_b.as_id())

    def test_topological_sort_container_constraints_no_dependencies(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test sorting views with no container dependencies maintains any order."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Two independent containers
        container_a = dm.Container(
            space="my_space",
            external_id="ContainerA",
            properties={
                "name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        container_b = dm.Container(
            space="my_space",
            external_id="ContainerB",
            properties={
                "value": dm.ContainerProperty(type=dm.Int32(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Two independent views
        view_a = _create_test_view(
            external_id="ViewA",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerA"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_b = _create_test_view(
            external_id="ViewB",
            properties={
                "value": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerB"),
                    container_property_identifier="value",
                    type=dm.Int32(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        toolkit_client_approval.append(dm.View, [view_a, view_b])
        toolkit_client_approval.append(dm.Container, [container_a, container_b])

        view_ids = [view_a.as_id(), view_b.as_id()]
        sorted_views = loader.topological_sort_container_constraints(view_ids)

        # Both views should be in the result (order doesn't matter since no dependencies)
        assert set(sorted_views) == set(view_ids)
        assert len(sorted_views) == 2

    def test_topological_sort_container_constraints_chain_dependency(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test sorting views with transitive chain dependencies: A -> B -> C."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Container A: No dependencies (root)
        container_a = dm.Container(
            space="my_space",
            external_id="ContainerA",
            properties={
                "name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Container B: Depends on A via DirectRelation
        container_b = dm.Container(
            space="my_space",
            external_id="ContainerB",
            properties={
                "parentA": dm.ContainerProperty(
                    type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerA")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Container C: Depends on B via RequiresConstraint
        container_c = dm.Container(
            space="my_space",
            external_id="ContainerC",
            properties={
                "value": dm.ContainerProperty(type=dm.Int32(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={"requiresB": dm.RequiresConstraint(require=dm.ContainerId("my_space", "ContainerB"))},
            description=None,
            name=None,
            indexes={},
        )

        # Create views
        view_a = _create_test_view(
            external_id="ViewA",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerA"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_b = _create_test_view(
            external_id="ViewB",
            properties={
                "parentA": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerB"),
                    container_property_identifier="parentA",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_c = _create_test_view(
            external_id="ViewC",
            properties={
                "value": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerC"),
                    container_property_identifier="value",
                    type=dm.Int32(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        toolkit_client_approval.append(dm.View, [view_a, view_b, view_c])
        toolkit_client_approval.append(dm.Container, [container_a, container_b, container_c])

        # Test with views in reverse order
        view_ids = [view_c.as_id(), view_b.as_id(), view_a.as_id()]
        sorted_views = loader.topological_sort_container_constraints(view_ids)

        # ViewA must come before ViewB, ViewB must come before ViewC
        assert sorted_views.index(view_a.as_id()) < sorted_views.index(view_b.as_id())
        assert sorted_views.index(view_b.as_id()) < sorted_views.index(view_c.as_id())

    def test_topological_sort_container_constraints_diamond_dependency(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test sorting views with diamond dependencies: A -> B, A -> C, B -> D, C -> D."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Container A: Root
        container_a = dm.Container(
            space="my_space",
            external_id="ContainerA",
            properties={
                "name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Container B: Depends on A
        container_b = dm.Container(
            space="my_space",
            external_id="ContainerB",
            properties={
                "parentA": dm.ContainerProperty(
                    type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerA")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Container C: Also depends on A
        container_c = dm.Container(
            space="my_space",
            external_id="ContainerC",
            properties={
                "value": dm.ContainerProperty(type=dm.Int32(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={"requiresA": dm.RequiresConstraint(require=dm.ContainerId("my_space", "ContainerA"))},
            description=None,
            name=None,
            indexes={},
        )

        # Container D: Depends on both B and C
        container_d = dm.Container(
            space="my_space",
            external_id="ContainerD",
            properties={
                "refB": dm.ContainerProperty(
                    type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerB")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={"requiresC": dm.RequiresConstraint(require=dm.ContainerId("my_space", "ContainerC"))},
            description=None,
            name=None,
            indexes={},
        )

        # Create views
        view_a = _create_test_view(
            external_id="ViewA",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerA"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_b = _create_test_view(
            external_id="ViewB",
            properties={
                "parentA": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerB"),
                    container_property_identifier="parentA",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_c = _create_test_view(
            external_id="ViewC",
            properties={
                "value": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerC"),
                    container_property_identifier="value",
                    type=dm.Int32(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_d = _create_test_view(
            external_id="ViewD",
            properties={
                "refB": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerD"),
                    container_property_identifier="refB",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        toolkit_client_approval.append(dm.View, [view_a, view_b, view_c, view_d])
        toolkit_client_approval.append(dm.Container, [container_a, container_b, container_c, container_d])

        # Test with views in random order
        view_ids = [view_d.as_id(), view_b.as_id(), view_c.as_id(), view_a.as_id()]
        sorted_views = loader.topological_sort_container_constraints(view_ids)

        # ViewA must come before B, C, and D
        assert sorted_views.index(view_a.as_id()) < sorted_views.index(view_b.as_id())
        assert sorted_views.index(view_a.as_id()) < sorted_views.index(view_c.as_id())
        assert sorted_views.index(view_a.as_id()) < sorted_views.index(view_d.as_id())

        # ViewB and ViewC must both come before ViewD
        assert sorted_views.index(view_b.as_id()) < sorted_views.index(view_d.as_id())
        assert sorted_views.index(view_c.as_id()) < sorted_views.index(view_d.as_id())

    def test_topological_sort_container_constraints_multiple_independent_chains(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test sorting views with multiple independent dependency chains: (A -> B) and (C -> D)."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # First chain: A -> B
        container_a = dm.Container(
            space="my_space",
            external_id="ContainerA",
            properties={
                "name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        container_b = dm.Container(
            space="my_space",
            external_id="ContainerB",
            properties={
                "parentA": dm.ContainerProperty(
                    type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerA")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Second chain: C -> D
        container_c = dm.Container(
            space="my_space",
            external_id="ContainerC",
            properties={
                "value": dm.ContainerProperty(type=dm.Int32(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        container_d = dm.Container(
            space="my_space",
            external_id="ContainerD",
            properties={
                "parentC": dm.ContainerProperty(
                    type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerC")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Create views
        view_a = _create_test_view(
            external_id="ViewA",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerA"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_b = _create_test_view(
            external_id="ViewB",
            properties={
                "parentA": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerB"),
                    container_property_identifier="parentA",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_c = _create_test_view(
            external_id="ViewC",
            properties={
                "value": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerC"),
                    container_property_identifier="value",
                    type=dm.Int32(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        view_d = _create_test_view(
            external_id="ViewD",
            properties={
                "parentC": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerD"),
                    container_property_identifier="parentC",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )

        toolkit_client_approval.append(dm.View, [view_a, view_b, view_c, view_d])
        toolkit_client_approval.append(dm.Container, [container_a, container_b, container_c, container_d])

        # Test with views interleaved
        view_ids = [view_d.as_id(), view_b.as_id(), view_c.as_id(), view_a.as_id()]
        sorted_views = loader.topological_sort_container_constraints(view_ids)

        # Within each chain, dependencies must be respected
        assert sorted_views.index(view_a.as_id()) < sorted_views.index(view_b.as_id())
        assert sorted_views.index(view_c.as_id()) < sorted_views.index(view_d.as_id())

        # All 4 views should be present
        assert len(sorted_views) == 4
