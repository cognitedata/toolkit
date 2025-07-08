import pytest
from cognite.client.data_classes.data_modeling import NodeApply, NodeApplyList, NodeList, NodeOrEdgeData, Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.tk_warnings import IgnoredValueWarning, catch_warnings


@pytest.fixture(scope="session")
def three_sources(toolkit_client: ToolkitClient, toolkit_space: Space) -> NodeList[InstanceSource]:
    nodes = NodeApplyList(
        [
            NodeApply(
                space=toolkit_space.space,
                external_id=f"toolkit_test_migration_{i}",
                sources=[
                    NodeOrEdgeData(
                        InstanceSource.get_source(),
                        {
                            "resourceType": "asset",
                            "id": i,
                            "preferredConsumerViewId": {
                                "space": "cdf_cdm",
                                "externalId": "CogniteAsset",
                                "version": "v1",
                            }
                            if i < 2
                            else {"invalid": "json"},
                        },
                    )
                ],
            )
            for i in range(3)
        ]
    )

    _ = toolkit_client.data_modeling.instances.apply(nodes)

    created = toolkit_client.data_modeling.instances.retrieve_nodes(nodes.as_ids(), node_cls=InstanceSource)
    assert len(created) == 3, "Expected 3 mappings to be created"
    return created


class TestInstanceSourceAPI:
    def test_retrieve_mappings(self, toolkit_client: ToolkitClient, three_sources: NodeList[InstanceSource]) -> None:
        ids = [instance_source.as_asset_centric_id() for instance_source in three_sources]

        with catch_warnings(IgnoredValueWarning) as warnings:
            retrieved = toolkit_client.migration.instance_source.retrieve(ids)

        assert retrieved.dump() == three_sources.dump(), "Failed to retrieve instance source using asset-centric IDs"
        assert len(warnings) == 1, "Expected one warning about invalid JSON in preferredConsumerViewId"
        has_preferred_consumer_view = [
            item.as_id() for item in retrieved if item.preferred_consumer_view_id is not None
        ]
        assert has_preferred_consumer_view == [item.as_id() for item in three_sources[:2]]
        no_preferred_consumer_view = [item.as_id() for item in retrieved if item.preferred_consumer_view_id is None]
        assert no_preferred_consumer_view == [three_sources[2].as_id()], (
            "Expected last item to have no preferred consumer view"
        )
