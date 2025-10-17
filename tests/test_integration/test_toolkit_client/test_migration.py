import pytest
from cognite.client.data_classes.data_modeling import NodeApply, NodeApplyList, NodeList, NodeOrEdgeData, Space, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import (
    CreatedSourceSystem,
    InstanceSource,
    ResourceViewMapping,
    ResourceViewMappingApply,
)
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


class TestResourceViewMappingAPI:
    def test_create_retrieve_list_delete(self, toolkit_client: ToolkitClient) -> None:
        source = ResourceViewMappingApply(
            external_id="test_view_source",
            resource_type="asset",
            view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            property_mapping={
                "name": "name",
                "description": "description",
            },
        )

        created: ResourceViewMapping | None = None
        try:
            created = toolkit_client.migration.resource_view_mapping.upsert(source)

            assert created.external_id == source.external_id

            retrieved = toolkit_client.migration.resource_view_mapping.retrieve(source.external_id)

            assert retrieved.external_id == source.external_id

            listed = toolkit_client.migration.resource_view_mapping.list(resource_type="asset")
            assert len(listed) > 0
            existing = {vs.external_id for vs in listed}
            assert source.external_id in existing, "Expected the created view source to be listed"

            deleted = toolkit_client.migration.resource_view_mapping.delete(source.external_id)

            assert deleted == created.as_id(), "Expected the deleted view source to match the created one"
        finally:
            if created:
                # Clean up by deleting the created view source if it exists
                toolkit_client.data_modeling.instances.delete(source.as_id())


@pytest.fixture(scope="session")
def created_source_system(toolkit_client: ToolkitClient, toolkit_space: Space) -> str:
    source = "toolkit_test_created_source_system"
    node = NodeApply(
        space=toolkit_space.space,
        external_id=source,
        sources=[
            NodeOrEdgeData(
                CreatedSourceSystem.get_source(),
                {"source": source},
            )
        ],
    )
    if not toolkit_client.data_modeling.instances.retrieve_nodes([node.as_id()]):
        _ = toolkit_client.data_modeling.instances.apply(node)
    return source


class TestCreatedSourceSystemAPI:
    @pytest.mark.usefixtures("created_source_system")
    def test_list(self, toolkit_client: ToolkitClient) -> None:
        created = toolkit_client.migration.created_source_system.list(limit=1)
        assert len(created) == 1

    def test_retrieve(self, created_source_system: str, toolkit_client: ToolkitClient) -> None:
        retrieved = toolkit_client.migration.created_source_system.retrieve([created_source_system])
        assert len(retrieved) == 1
        node = retrieved[0]
        assert node.external_id == created_source_system
        assert node.source == created_source_system
