import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    EdgeRequest,
    InstanceSource,
    MultiEdgeProperty,
    NodeReference,
    NodeRequest,
    Space,
    TextProperty,
    ViewCorePropertyRequest,
    ViewReference,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.storageio import InstanceIO
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceViewSelector, SelectedView


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
) -> ViewResponse:
    """View for nodes with name, outwards, and inwards edge properties."""
    view_id = ViewReference(space=toolkit_space.space, external_id="test_node_view_with_edges", version="v1")
    edge_type = NodeReference(space=toolkit_space.space, external_id=f"{view_id.external_id}.edge")
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
        return retrieved[0]
    created = toolkit_client.tool.views.create([view])
    assert created, "Failed to create or retrieve node view"
    return created[0]


@pytest.fixture(scope="module")
def two_nodes_with_edge(
    toolkit_client: ToolkitClient,
    toolkit_space: Space,
    node_view_with_edges: ViewResponse,
) -> list[InstanceSlimDefinition]:
    """Create two nodes with an edge connecting them."""
    node_view_source = node_view_with_edges.as_id()
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
        start_node=NodeReference(space=toolkit_space.space, external_id=node_a.external_id),
        end_node=NodeReference(space=toolkit_space.space, external_id=node_b.external_id),
        type=NodeReference(space=toolkit_space.space, external_id=f"{node_view_source.external_id}.edge"),
    )
    return toolkit_client.tool.instances.create([node_a, node_b, connecting_edge])


class TestInstanceIO:
    @pytest.mark.usefixtures("two_nodes_with_edge")
    def test_stream_nodes_with_edges(
        self,
        toolkit_client: ToolkitClient,
        node_view_with_edges: ViewResponse,
    ) -> None:
        """Test that streaming nodes with include_edges=True includes the edge information."""
        view = node_view_with_edges

        selector = InstanceViewSelector(
            view=SelectedView(
                space=view.space,
                external_id=view.external_id,
                version=view.version,
            ),
            instance_type="node",
            include_edges=True,
        )

        io = InstanceIO(toolkit_client)
        pages = list(io.stream_data(selector))

        results = [instance for page in pages for instance in page.items]
        assert len(results) == 3, f"Expected 3 instances (2 nodes + 1 edge), got {len(results)}"
