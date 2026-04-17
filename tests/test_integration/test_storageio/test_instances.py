import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, InstanceDefinitionId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    EdgeRequest,
    InstanceSource,
    MultiEdgeProperty,
    NodeId,
    NodeRequest,
    Space,
    TextProperty,
    ViewCorePropertyRequest,
    ViewId,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import create_infield_schedule_selector
from cognite_toolkit._cdf_tk.dataio import InstanceIO
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceViewSelector, SelectedView


@pytest.fixture(scope="module")
def node_container(toolkit_client: ToolkitClient, toolkit_space: Space) -> ContainerResponse:
    """Container for nodes with a name property."""
    client = toolkit_client
    container = ContainerRequest(
        space=toolkit_space.space,
        external_id="test_stream_nodes_node_container",
        name="Test Node Container",
        description="Node container for stream nodes with edges test",
        used_for="node",
        properties={
            "name": ContainerPropertyDefinition(type=TextProperty()),
        },
    )
    retrieved = client.tool.containers.retrieve([container.as_id()])
    if retrieved:
        return retrieved[0]
    created = client.tool.containers.create([container])
    assert created, "Failed to create or retrieve node container"
    return created[0]


@pytest.fixture(scope="module")
def node_view_with_edges(
    toolkit_client: ToolkitClient,
    toolkit_space: Space,
    node_container: ContainerResponse,
) -> tuple[ViewResponse, NodeId]:
    """View for nodes with name, outwards, and inwards edge properties."""
    view_id = ViewId(space=toolkit_space.space, external_id="test_node_view_with_edges", version="v1")
    edge_type = NodeId(space=toolkit_space.space, external_id=f"{view_id.external_id}.edge")
    view = ViewRequest(
        space=toolkit_space.space,
        external_id=view_id.external_id,
        version=view_id.version,
        name="Test View for Nodes with Edges",
        description="View for testing streaming nodes with edges",
        properties={
            "name": ViewCorePropertyRequest(
                container=node_container.as_id(),
                container_property_identifier="name",
            ),
            "outwards": MultiEdgeProperty(
                type=edge_type,
                source=view_id,
                direction="outwards",
            ),
            "inwards": MultiEdgeProperty(
                type=edge_type,
                source=view_id,
                direction="inwards",
            ),
        },
    )
    retrieved = toolkit_client.tool.views.retrieve([view.as_id()])
    if retrieved:
        return retrieved[0], edge_type
    created = toolkit_client.tool.views.create([view])
    assert created, "Failed to create or retrieve node view"
    return created[0], edge_type


@pytest.fixture(scope="module")
def two_nodes_with_edge(
    toolkit_client: ToolkitClient,
    toolkit_space: Space,
    node_view_with_edges: tuple[ViewResponse, NodeId],
) -> list[InstanceSlimDefinition]:
    """Create two nodes with an edge connecting them."""
    view, _ = node_view_with_edges

    node_view_source = view.as_id()
    prefix = "test_stream_nodes_with_edges_"
    node_a = NodeRequest(
        space=toolkit_space.space,
        external_id=f"{prefix}node_a",
        sources=[
            InstanceSource(
                source=node_view_source,
                properties={"name": "Node A"},
            )
        ],
    )
    node_b = NodeRequest(
        space=toolkit_space.space,
        external_id=f"{prefix}node_b",
        sources=[
            InstanceSource(
                source=node_view_source,
                properties={"name": "Node B"},
            )
        ],
    )
    connecting_edge = EdgeRequest(
        space=toolkit_space.space,
        external_id=f"{prefix}edge_a_to_b",
        start_node=NodeId(space=toolkit_space.space, external_id=node_a.external_id),
        end_node=NodeId(space=toolkit_space.space, external_id=node_b.external_id),
        type=NodeId(space=toolkit_space.space, external_id=f"{node_view_source.external_id}.edge"),
    )
    return toolkit_client.tool.instances.create([node_a, node_b, connecting_edge])


@pytest.fixture(scope="session")
def infield_apm_app_data_schedule_populated(
    toolkit_client: ToolkitClient, toolkit_space: Space
) -> list[InstanceDefinitionId]:
    instance_space = toolkit_space.space
    template = ViewId(space="cdf_apm", external_id="Template", version="v8")
    item = ViewId(space="cdf_apm", external_id="TemplateItem", version="v7")
    schedule = ViewId(space="cdf_apm", external_id="Schedule", version="v4")
    template_to_item_type = NodeId(space="cdf_apm", external_id="referenceTemplateItems")
    item_to_schedule_type = NodeId(space="cdf_apm", external_id="referenceSchedules")

    template_id = NodeId(space=instance_space, external_id="infield_apm_app_data_schedule_populated_template")
    item_id = NodeId(space=instance_space, external_id="infield_apm_app_data_schedule_populated_item")
    schedule_id = NodeId(space=instance_space, external_id="infield_apm_app_data_schedule_populated_schedule")

    instances = [
        NodeRequest(
            space=template_id.space,
            external_id=template_id.external_id,
            sources=[
                InstanceSource(
                    source=template,
                    properties={"title": "Toolkit Integration test template"},
                )
            ],
        ),
        EdgeRequest(
            space=instance_space,
            external_id=f"{template_id.external_id}_{item_id.external_id}",
            start_node=template_id,
            end_node=item_id,
            type=template_to_item_type,
        ),
        NodeRequest(
            space=item_id.space,
            external_id=item_id.external_id,
            sources=[
                InstanceSource(
                    source=item,
                    properties={"title": "Toolkit Integration test template item"},
                )
            ],
        ),
        EdgeRequest(
            space=instance_space,
            external_id=f"{item_id.external_id}_{schedule_id.external_id}",
            start_node=item_id,
            end_node=schedule_id,
            type=item_to_schedule_type,
        ),
        NodeRequest(
            space=schedule_id.space,
            external_id=schedule_id.external_id,
            sources=[
                InstanceSource(
                    source=schedule,
                    properties={"status": "confirmed"},
                )
            ],
        ),
    ]
    created_instances = toolkit_client.tool.instances.create(instances)
    assert len(created_instances) == len(created_instances), "Failed to create instance"
    return [template_id, schedule_id, *[edge.as_id() for edge in created_instances if edge.instance_type == "edge"]]


class TestInstanceIO:
    @pytest.mark.usefixtures("two_nodes_with_edge")
    def test_stream_nodes_with_edges(
        self,
        toolkit_client: ToolkitClient,
        node_view_with_edges: tuple[ViewResponse, NodeId],
    ) -> None:
        """Test that streaming nodes with include_edges=True includes the edge information."""
        view, edge_type = node_view_with_edges

        selector = InstanceViewSelector(
            view=SelectedView(
                space=view.space,
                external_id=view.external_id,
                version=view.version,
            ),
            instance_type="node",
            edge_types=(EdgeTypeId(type=edge_type, direction="outwards"),),
        )

        io = InstanceIO(toolkit_client)
        pages = list(io.stream_data(selector))

        results = [instance for page in pages for instance in page.items]
        assert len(results) == 3, f"Expected 3 instances (2 nodes + 1 edge), got {len(results)}"

    def test_stream_nodes_by_infield_query(
        self,
        toolkit_client: ToolkitClient,
        infield_apm_app_data_schedule_populated: list[InstanceDefinitionId],
    ) -> None:
        instance_spaces = list(set(instance.space for instance in infield_apm_app_data_schedule_populated))
        assert len(instance_spaces) == 1, "Expected all instances to be in the same space"
        client = toolkit_client
        selector = create_infield_schedule_selector(instance_space=instance_spaces[0])
        io = InstanceIO(client)
        pages = list(io.stream_data(selector))

        actual = [instance.item.as_id() for page in pages for instance in page.items]
        expected = infield_apm_app_data_schedule_populated
        assert set(actual) == set(expected), f"Expected edge instances {expected[1:]}, got {actual[1:]}"
