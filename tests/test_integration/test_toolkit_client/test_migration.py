import pytest
from cognite.client.data_classes.data_modeling import NodeApply, NodeApplyList, NodeList, NodeOrEdgeData, Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import Mapping


@pytest.fixture(scope="session")
def three_mappings(toolkit_client: ToolkitClient, toolkit_space: Space) -> NodeList[Mapping]:
    nodes = NodeApplyList(
        [
            NodeApply(
                space=toolkit_space.space,
                external_id=f"toolkit_test_migration_{i}",
                sources=[
                    NodeOrEdgeData(
                        Mapping.get_source(),
                        {
                            "resourceType": "asset",
                            "id": i,
                        },
                    )
                ],
            )
            for i in range(3)
        ]
    )

    _ = toolkit_client.data_modeling.instances.apply(nodes)

    created = toolkit_client.data_modeling.instances.retrieve_nodes(nodes.as_ids(), node_cls=Mapping)
    assert len(created) == 3, "Expected 3 mappings to be created"
    return created


class TestMappingAPI:
    @pytest.mark.skip(
        reason="The mapping API is outdated and needs to be updated with the round 2 of the Migration model."
    )
    def test_retrieve_mappings(self, toolkit_client: ToolkitClient, three_mappings: NodeList[Mapping]) -> None:
        ids = [mapping.as_asset_centric_id() for mapping in three_mappings]

        retrieved = toolkit_client.migration.mapping.retrieve(ids)

        assert retrieved.dump() == three_mappings.dump(), "Failed to retrieve mappings using asset-centric IDs"
