from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.api.instances import INSTANCE_UPSERT_ENDPOINT
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ConstraintOrIndexState,
    ContainerPropertyDefinition,
    ContainerResponse,
    DirectNodeRelation,
    InstanceSource,
    NodeRequest,
    ViewCorePropertyResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.resource_ios import NodeCRUD


def _loader() -> NodeCRUD:
    # None of the tests below need real API access: either the schema cache is populated directly
    # (testing _compute_deploy_batches in isolation), or the client is mocked with
    # monkeypatch_toolkit_client (testing _lookup_constrained_properties itself).
    return NodeCRUD(MagicMock(spec=ToolkitClient), Path("build_dir"), MagicMock(spec=Console))


def _container(
    external_id: str, properties: dict[str, ContainerPropertyDefinition], space: str = "sp"
) -> ContainerResponse:
    return ContainerResponse(
        space=space,
        external_id=external_id,
        properties=properties,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        used_for="node",
        constraints={},
        description=None,
        name=None,
        indexes={},
    )


def _view_property(
    container_id: ContainerId, container_property_identifier: str, prop_type: DirectNodeRelation
) -> ViewCorePropertyResponse:
    return ViewCorePropertyResponse(
        container=container_id,
        container_property_identifier=container_property_identifier,
        type=prop_type,
        nullable=True,
        immutable=False,
        auto_increment=False,
        constraint_state=ConstraintOrIndexState(),
    )


def _view(
    external_id: str,
    space: str,
    properties: Mapping[str, ViewCorePropertyResponse],
    mapped_containers: list[ContainerId],
) -> ViewResponse:
    return ViewResponse(
        space=space,
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
        mapped_containers=mapped_containers,
    )


def _node(space: str, external_id: str, source: ContainerId | ViewId, properties: dict) -> NodeRequest:
    return NodeRequest(
        space=space,
        external_id=external_id,
        sources=[InstanceSource(source=source, properties=properties)],
    )


class TestNodeCRUDComputeDeployBatches:
    """Tests for the node ordering/batching logic, given an already-resolved schema.

    The schema cache (loader._constrained_properties_by_source) is populated directly, so these tests
    do not need a real or mocked client: resolving the schema itself is tested separately in
    TestNodeCRUDLookupConstrainedProperties.
    """

    @pytest.mark.parametrize(
        "ref_value, target_ids",
        [
            pytest.param({"space": "sp", "externalId": "target_node"}, ["target_node"], id="single_value"),
            pytest.param(
                [{"space": "sp", "externalId": "target_1"}, {"space": "sp", "externalId": "target_2"}],
                ["target_1", "target_2"],
                id="list_value",
            ),
        ],
    )
    def test_container_constrained_direct_relation_orders_targets_before_referrer(
        self, ref_value: Any, target_ids: list[str]
    ) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")
        target_container_id = ContainerId(space="sp", external_id="Target")
        referrer = _node("sp", "referrer_node", referrer_container_id, {"ref": ref_value})
        targets = [_node("sp", target_id, target_container_id, {}) for target_id in target_ids]

        loader = _loader()
        loader._constrained_properties_by_source = {referrer_container_id: {"ref"}, target_container_id: set()}
        batches = loader._compute_deploy_batches([referrer, *targets])

        flat_ids = [node.external_id for batch in batches for node in batch]
        for target_id in target_ids:
            assert flat_ids.index(target_id) < flat_ids.index("referrer_node")

    def test_no_ordering_forced_when_relation_is_not_known_to_be_container_constrained(self) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")
        referrer = _node(
            "sp", "referrer_node", referrer_container_id, {"ref": {"space": "sp", "externalId": "target_node"}}
        )
        target = _node("sp", "target_node", referrer_container_id, {})

        loader = _loader()
        # An empty set of constrained properties: whether that is because the relation is genuinely
        # unconstrained, or because the source could not be resolved at all, neither forces an
        # ordering (see TestNodeCRUDLookupConstrainedProperties for how each case populates the cache).
        # The referrer is listed first; with no ordering edge, insertion order is preserved.
        loader._constrained_properties_by_source = {referrer_container_id: set()}
        batches = loader._compute_deploy_batches([referrer, target])

        flat_ids = [node.external_id for batch in batches for node in batch]
        assert flat_ids.index("referrer_node") < flat_ids.index("target_node")

    def test_self_referential_container_constraint_orders_nodes_within_same_container(self) -> None:
        category_container_id = ContainerId(space="sp", external_id="Category")
        parent_node = _node("sp", "parent_node", category_container_id, {})
        child_node = _node(
            "sp", "child_node", category_container_id, {"parent": {"space": "sp", "externalId": "parent_node"}}
        )

        loader = _loader()
        loader._constrained_properties_by_source = {category_container_id: {"parent"}}
        batches = loader._compute_deploy_batches([child_node, parent_node])

        flat_ids = [node.external_id for batch in batches for node in batch]
        assert flat_ids.index("parent_node") < flat_ids.index("child_node")

    def test_many_referrers_split_across_batches_after_target(self) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")
        target_container_id = ContainerId(space="sp", external_id="Target")
        referrer_count = INSTANCE_UPSERT_ENDPOINT.item_limit + 25
        referrers = [
            _node("sp", f"referrer_{i}", referrer_container_id, {"ref": {"space": "sp", "externalId": "target_node"}})
            for i in range(referrer_count)
        ]
        target = _node("sp", "target_node", target_container_id, {})

        loader = _loader()
        loader._constrained_properties_by_source = {referrer_container_id: {"ref"}, target_container_id: set()}
        batches = loader._compute_deploy_batches([*referrers, target])

        assert len(batches) > 1, "Should split into multiple batches given the item limit"
        assert batches[0][0].external_id == "target_node", "Node being depended on must be sent first"
        flat_ids = [node.external_id for batch in batches for node in batch]
        for referrer in referrers:
            assert flat_ids.index("target_node") < flat_ids.index(referrer.external_id)

    def test_cycle_of_mutually_referring_nodes_stays_in_one_batch(self) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")
        node_a = _node("sp", "node_a", referrer_container_id, {"ref": {"space": "sp", "externalId": "node_b"}})
        node_b = _node("sp", "node_b", referrer_container_id, {"ref": {"space": "sp", "externalId": "node_a"}})

        loader = _loader()
        loader._constrained_properties_by_source = {referrer_container_id: {"ref"}}
        batches = loader._compute_deploy_batches([node_a, node_b])

        assert len(batches) == 1, "A cycle of mutually referring nodes must stay in a single batch"
        assert {node.external_id for node in batches[0]} == {"node_a", "node_b"}

    def test_direct_relation_loaded_from_raw_dict_is_picked_up(self) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")

        loader = _loader()
        referrer = loader.load_resource(
            {
                "space": "sp",
                "externalId": "referrer_node",
                "sources": [
                    {
                        "source": {"type": "container", "space": "sp", "externalId": "Referrer"},
                        "properties": {"ref": {"space": "sp", "externalId": "target_node"}},
                    }
                ],
            }
        )
        target = loader.load_resource({"space": "sp", "externalId": "target_node"})
        loader._constrained_properties_by_source = {referrer_container_id: {"ref"}}

        batches = loader._compute_deploy_batches([referrer, target])

        # Pin down the shape a YAML-loaded direct relation value takes under pydantic's smart union,
        # rather than assuming it: this is what NodeCRUD._as_node_ids must be able to read.
        assert referrer.sources is not None
        assert referrer.sources[0].properties is not None
        assert isinstance(referrer.sources[0].properties["ref"], dict)

        flat_ids = [node.external_id for batch in batches for node in batch]
        assert flat_ids.index("target_node") < flat_ids.index("referrer_node")


class TestNodeCRUDLookupConstrainedProperties:
    """Tests for resolving which properties are container-constrained direct relations, per source."""

    def test_resolves_constrained_properties_from_container_schema(self) -> None:
        container_id = ContainerId(space="sp", external_id="Category")
        container = _container(
            "Category",
            {
                "parent": ContainerPropertyDefinition(type=DirectNodeRelation(container=container_id), nullable=True),
                "unconstrained": ContainerPropertyDefinition(type=DirectNodeRelation(container=None), nullable=True),
            },
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [container]
            loader = NodeCRUD(client, Path("build_dir"), None)
            loader._lookup_constrained_properties({container_id})

        assert loader._constrained_properties_by_source[container_id] == {"parent"}

    def test_resolves_constrained_properties_from_view_schema_without_container_lookup(self) -> None:
        container_id = ContainerId(space="sp", external_id="Category")
        view = _view(
            "CategoryView",
            "sp",
            {"parent": _view_property(container_id, "parent", DirectNodeRelation(container=container_id))},
            mapped_containers=[container_id],
        )
        view_id = view.as_id()

        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [view]
            loader = NodeCRUD(client, Path("build_dir"), None)
            loader._lookup_constrained_properties({view_id})

        assert loader._constrained_properties_by_source[view_id] == {"parent"}
        # The constraint is read off the view's own property type, no container lookup is needed.
        client.tool.containers.retrieve.assert_not_called()

    def test_unresolvable_source_is_not_cached(self) -> None:
        unknown_container_id = ContainerId(space="sp", external_id="Unknown")

        with monkeypatch_toolkit_client() as client:
            # The container cannot be resolved (not found, or no read access).
            client.tool.containers.retrieve.return_value = []
            loader = NodeCRUD(client, Path("build_dir"), None)
            loader._lookup_constrained_properties({unknown_container_id})

        assert unknown_container_id not in loader._constrained_properties_by_source
