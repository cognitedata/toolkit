from pathlib import Path
from typing import Any

import pytest

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
    external_id: str, space: str, properties: dict[str, ViewCorePropertyResponse], mapped_containers: list[ContainerId]
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


class TestNodeCRUDDeployBatching:
    @pytest.mark.parametrize(
        "ref_value, target_ids, is_list",
        [
            pytest.param({"space": "sp", "externalId": "target_node"}, ["target_node"], None, id="single_value"),
            pytest.param(
                [{"space": "sp", "externalId": "target_1"}, {"space": "sp", "externalId": "target_2"}],
                ["target_1", "target_2"],
                True,
                id="list_value",
            ),
        ],
    )
    def test_container_constrained_direct_relation_orders_targets_before_referrer(
        self, ref_value: Any, target_ids: list[str], is_list: bool | None
    ) -> None:
        target_container = ContainerId(space="sp", external_id="Target")
        referrer_container = _container(
            "Referrer",
            {"ref": ContainerPropertyDefinition(type=DirectNodeRelation(container=target_container, list=is_list))},
        )
        referrer = _node("sp", "referrer_node", referrer_container.as_id(), {"ref": ref_value})
        targets = [_node("sp", target_id, target_container, {}) for target_id in target_ids]

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [referrer_container, _container("Target", {})]
            loader = NodeCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([referrer, *targets])

        flat_ids = [node.external_id for batch in batches for node in batch]
        for target_id in target_ids:
            assert flat_ids.index(target_id) < flat_ids.index("referrer_node")

    @pytest.mark.parametrize(
        "resolvable",
        [
            pytest.param(True, id="unconstrained_direct_relation"),
            pytest.param(False, id="unresolvable_source"),
        ],
    )
    def test_no_ordering_forced_when_relation_is_not_known_to_be_container_constrained(self, resolvable: bool) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")
        referrer_container = _container(
            "Referrer",
            {"ref": ContainerPropertyDefinition(type=DirectNodeRelation(container=None), nullable=True)},
        )
        referrer = _node(
            "sp", "referrer_node", referrer_container_id, {"ref": {"space": "sp", "externalId": "target_node"}}
        )
        target = _node("sp", "target_node", referrer_container_id, {})

        with monkeypatch_toolkit_client() as client:
            # Either the schema resolves and the relation is genuinely unconstrained, or the source
            # cannot be resolved at all (not found, or no read access) -- neither forces an ordering.
            client.tool.containers.retrieve.return_value = [referrer_container] if resolvable else []
            loader = NodeCRUD(client, Path("build_dir"), None)
            # The referrer is listed first; with no ordering edge, insertion order is preserved.
            batches = loader._compute_deploy_batches([referrer, target])

        flat_ids = [node.external_id for batch in batches for node in batch]
        assert flat_ids.index("referrer_node") < flat_ids.index("target_node")

    @pytest.mark.parametrize(
        "use_view_source",
        [
            pytest.param(False, id="via_container_source"),
            pytest.param(True, id="via_view_source"),
        ],
    )
    def test_self_referential_container_constraint_orders_nodes_within_same_container(
        self, use_view_source: bool
    ) -> None:
        category_container_id = ContainerId(space="sp", external_id="Category")
        container = _container(
            "Category",
            {
                "parent": ContainerPropertyDefinition(
                    type=DirectNodeRelation(container=category_container_id), nullable=True
                )
            },
        )

        with monkeypatch_toolkit_client() as client:
            if use_view_source:
                view = _view(
                    "CategoryView",
                    "sp",
                    {
                        "parent": _view_property(
                            category_container_id, "parent", DirectNodeRelation(container=category_container_id)
                        )
                    },
                    mapped_containers=[category_container_id],
                )
                source: ContainerId | ViewId = view.as_id()
                client.tool.views.retrieve.return_value = [view]
            else:
                source = category_container_id
                client.tool.containers.retrieve.return_value = [container]

            parent_node = _node("sp", "parent_node", source, {})
            child_node = _node("sp", "child_node", source, {"parent": {"space": "sp", "externalId": "parent_node"}})

            loader = NodeCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([child_node, parent_node])

        flat_ids = [node.external_id for batch in batches for node in batch]
        assert flat_ids.index("parent_node") < flat_ids.index("child_node")
        if use_view_source:
            # The constraint is read off the view's own property type, no container lookup is needed.
            client.tool.containers.retrieve.assert_not_called()

    def test_many_referrers_split_across_batches_after_target(self) -> None:
        target_container = ContainerId(space="sp", external_id="Target")
        referrer_container = _container(
            "Referrer",
            {"ref": ContainerPropertyDefinition(type=DirectNodeRelation(container=target_container), nullable=True)},
        )
        referrer_count = INSTANCE_UPSERT_ENDPOINT.item_limit + 25
        referrers = [
            _node(
                "sp",
                f"referrer_{i}",
                referrer_container.as_id(),
                {"ref": {"space": "sp", "externalId": "target_node"}},
            )
            for i in range(referrer_count)
        ]
        target = _node("sp", "target_node", target_container, {})

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [referrer_container, _container("Target", {})]
            loader = NodeCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([*referrers, target])

        assert len(batches) > 1, "Should split into multiple batches given the item limit"
        assert batches[0][0].external_id == "target_node", "Node being depended on must be sent first"
        flat_ids = [node.external_id for batch in batches for node in batch]
        for referrer in referrers:
            assert flat_ids.index("target_node") < flat_ids.index(referrer.external_id)

    def test_cycle_of_mutually_referring_nodes_stays_in_one_batch(self) -> None:
        referrer_container_id = ContainerId(space="sp", external_id="Referrer")
        referrer_container = _container(
            "Referrer",
            {"ref": ContainerPropertyDefinition(type=DirectNodeRelation(container=referrer_container_id))},
        )
        node_a = _node("sp", "node_a", referrer_container_id, {"ref": {"space": "sp", "externalId": "node_b"}})
        node_b = _node("sp", "node_b", referrer_container_id, {"ref": {"space": "sp", "externalId": "node_a"}})

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [referrer_container]
            loader = NodeCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([node_a, node_b])

        assert len(batches) == 1, "A cycle of mutually referring nodes must stay in a single batch"
        assert {node.external_id for node in batches[0]} == {"node_a", "node_b"}

    def test_direct_relation_loaded_from_raw_dict_is_picked_up(self) -> None:
        target_container = ContainerId(space="sp", external_id="Target")
        referrer_container = _container(
            "Referrer",
            {"ref": ContainerPropertyDefinition(type=DirectNodeRelation(container=target_container), nullable=True)},
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [referrer_container, _container("Target", {})]
            loader = NodeCRUD(client, Path("build_dir"), None)
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

            batches = loader._compute_deploy_batches([referrer, target])

        # Pin down the shape a YAML-loaded direct relation value takes under pydantic's smart union,
        # rather than assuming it: this is what NodeCRUD._as_node_ids must be able to read.
        assert isinstance(referrer.sources[0].properties["ref"], dict)

        flat_ids = [node.external_id for batch in batches for node in batch]
        assert flat_ids.index("target_node") < flat_ids.index("referrer_node")
