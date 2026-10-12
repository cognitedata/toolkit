import json
from typing import Any

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.request_classes.filters import ThreeDNodeNameFilter, ThreeDNodePropertyFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequestId,
    AssetMappingDMRequestId,
)


@pytest.fixture()
def asset_mapping_classic() -> dict[str, Any]:
    return {
        "nodeId": 123,
        "assetId": 456,
        "treeIndex": 1,
        "subtreeSize": 10,
    }


@pytest.fixture()
def asset_mapping_dm() -> dict[str, Any]:
    return {
        "nodeId": 123,
        "assetInstanceId": {
            "space": "my_space",
            "externalId": "my_external_id",
        },
        "treeIndex": 1,
        "subtreeSize": 10,
    }


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestAssetsMappingsClassic:
    def test_create(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        asset_mapping_classic: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings")
        respx_mock.post(url).respond(status_code=200, json={"items": [asset_mapping_classic]})

        responses = toolkit_client.tool.three_d.asset_mappings_classic.create(
            [AssetMappingClassicRequestId(nodeId=123, assetId=456, modelId=37, revisionId=42)]
        )
        assert len(responses) == 1
        response = responses[0]
        assert response.dump() == asset_mapping_classic
        assert response.model_id == 37
        assert response.revision_id == 42

    def test_delete(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings/delete")
        respx_mock.post(url).respond(status_code=200, json={})

        toolkit_client.tool.three_d.asset_mappings_classic.delete(
            [AssetMappingClassicRequestId(nodeId=123, assetId=456, modelId=37, revisionId=42)]
        )

    @pytest.mark.parametrize(
        "args, expected_error",
        [
            pytest.param({"limit": -10}, "Limit must be between 1 and 1000, got -10.", id="negative limit"),
            pytest.param({"limit": 0}, "Limit must be between 1 and 1000, got 0.", id="zero limit"),
            pytest.param({"limit": 1001}, "Limit must be between 1 and 1000, got 1001.", id="excessive limit"),
        ],
    )
    def test_iterate_invalid_inputs(
        self, args: dict[str, Any], expected_error: str, toolkit_client: ToolkitClient
    ) -> None:
        with pytest.raises(ValueError, match=expected_error):
            toolkit_client.tool.three_d.asset_mappings_classic.paginate(model_id=37, revision_id=42, **args)

    def test_create_empty_list(
        self,
        toolkit_client: ToolkitClient,
    ) -> None:
        responses = toolkit_client.tool.three_d.asset_mappings_classic.create([])
        assert len(responses) == 0


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestAssetsMappingsDM:
    def test_create_dm(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        asset_mapping_dm: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings")
        respx_mock.post(url).respond(status_code=200, json={"items": [asset_mapping_dm]})

        responses = toolkit_client.tool.three_d.asset_mappings_dm.create(
            [
                AssetMappingDMRequestId(
                    nodeId=123,
                    assetInstanceId=NodeId(space="my_space", externalId="my_external_id"),
                    modelId=37,
                    revisionId=42,
                )
            ],
            object_3d_space="object_space",
            cad_node_space="cad_space",
        )
        assert len(responses) == 1
        response = responses[0]
        assert response.dump() == asset_mapping_dm
        assert response.model_id == 37
        assert response.revision_id == 42

    def test_delete_dm(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings/delete")
        respx_mock.post(url).respond(status_code=200, json={})

        toolkit_client.tool.three_d.asset_mappings_dm.delete(
            [
                AssetMappingDMRequestId(
                    nodeId=123,
                    assetInstanceId=NodeId(space="my_space", externalId="my_external_id"),
                    modelId=37,
                    revisionId=42,
                )
            ],
            object_3d_space="object_space",
            cad_node_space="cad_space",
        )

    def test_create_dm_many_in_different_models(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url_model_37 = config.create_api_url("/3d/models/37/revisions/42/mappings")
        url_model_38 = config.create_api_url("/3d/models/38/revisions/42/mappings")

        def callback(request: httpx2.Request) -> httpx2.Response:
            payload = json.loads(request.content.decode("utf-8"))
            items = payload.get("items", [])
            for item in items:
                item["treeIndex"] = 1
                item["subtreeSize"] = 10
            return httpx2.Response(status_code=200, json={"items": items})

        respx_mock.post(url_model_37).mock(side_effect=callback)
        respx_mock.post(url_model_38).mock(side_effect=callback)

        mappings = [
            AssetMappingDMRequestId(
                nodeId=i,
                assetInstanceId=NodeId(space="space", externalId=f"external_{i}"),
                modelId=37 if i % 2 == 0 else 38,
                revisionId=42,
            )
            for i in range(300)
        ]

        responses = toolkit_client.tool.three_d.asset_mappings_dm.create(
            mappings,
            object_3d_space="object_space",
            cad_node_space="cad_space",
        )

        assert len(responses) == 300
        assert respx_mock.calls.call_count == 4, (
            "Expected 4 calls for 2 models with 150 mappings each and batch size of 100"
        )
        items_per_request = [
            len(json.loads(call.request.content.decode("utf-8"))["items"]) for call in respx_mock.calls
        ]
        assert items_per_request == [100, 50, 100, 50], (
            f"Unexpected distribution of items per request: {items_per_request}"
        )

    def test_iterate(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        asset_mapping_dm: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings/list")
        respx_mock.post(url).respond(
            status_code=200,
            json={
                "items": [asset_mapping_dm],
                "nextCursor": "next",
            },
        )

        page = toolkit_client.tool.three_d.asset_mappings_dm.paginate(model_id=37, revision_id=42, limit=100)
        assert len(page.items) == 1
        assert page.items[0].dump() == asset_mapping_dm
        assert page.next_cursor == "next"

    def test_list_with_pagination(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        asset_mapping_dm: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings/list")
        respx_mock.post(url).side_effect = [
            respx.MockResponse(status_code=200, json={"items": [asset_mapping_dm], "nextCursor": "cursor1"}),
            respx.MockResponse(status_code=200, json={"items": [asset_mapping_dm], "nextCursor": None}),
        ]

        results = toolkit_client.tool.three_d.asset_mappings_dm.list(model_id=37, revision_id=42, limit=None)
        assert len(results) == 2
        assert results[0].dump() == asset_mapping_dm
        assert results[1].dump() == asset_mapping_dm

        assert respx_mock.calls.call_count == 2


@pytest.fixture()
def three_d_node() -> dict[str, Any]:
    return {
        "id": 1000,
        "treeIndex": 3,
        "parentId": 2,
        "depth": 2,
        "name": "Node name",
        "subtreeSize": 4,
        "properties": {"category1": {"property1": "value1"}},
        "boundingBox": {"max": [1.0, 2.0, 3.0], "min": [0.0, 0.0, 0.0]},
    }


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestThreeDNodes:
    def test_retrieve(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        three_d_node: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes/byids")
        respx_mock.post(url).respond(status_code=200, json={"items": [three_d_node]})

        responses = toolkit_client.tool.three_d.nodes.retrieve(model_id=37, revision_id=42, ids=[1000, 1001])

        request_body = json.loads(respx_mock.calls.last.request.content)
        assert request_body == {"items": [{"id": 1000}, {"id": 1001}]}
        assert len(responses) == 1
        response = responses[0]
        assert response.dump() == three_d_node
        assert response.model_id == 37
        assert response.revision_id == 42

    def test_retrieve_chunks_by_1000(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes/byids")
        respx_mock.post(url).respond(status_code=200, json={"items": []})

        toolkit_client.tool.three_d.nodes.retrieve(model_id=37, revision_id=42, ids=list(range(1001)))

        assert respx_mock.calls.call_count == 2
        batch_sizes = [len(json.loads(call.request.content)["items"]) for call in respx_mock.calls]
        assert batch_sizes == [1000, 1]

    def test_list_query(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        three_d_node: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes")
        respx_mock.get(url).respond(status_code=200, json={"items": [three_d_node]})

        responses = toolkit_client.tool.three_d.nodes.list(
            model_id=37,
            revision_id=42,
            node_id=5,
            depth=1,
            sort_by_node_id=True,
            partition="1/2",
            properties={"Item": {"Type": "Box"}},
            limit=10,
        )

        assert dict(respx_mock.calls.last.request.url.params) == {
            "limit": "10",
            "nodeId": "5",
            "depth": "1",
            "sortByNodeId": "true",
            "partition": "1/2",
            "properties": '{"Item":{"Type":"Box"}}',
        }
        assert responses[0].dump() == three_d_node
        assert responses[0].model_id == 37
        assert responses[0].revision_id == 42

    def test_list_partition_requires_sort_by_node_id(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ValueError, match="sort_by_node_id"):
            toolkit_client.tool.three_d.nodes.paginate(model_id=37, revision_id=42, partition="1/2")

    def test_list_follows_cursor(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        three_d_node: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes")
        respx_mock.get(url).side_effect = [
            respx.MockResponse(status_code=200, json={"items": [three_d_node], "nextCursor": "cursor1"}),
            respx.MockResponse(status_code=200, json={"items": [three_d_node]}),
        ]

        results = toolkit_client.tool.three_d.nodes.list(model_id=37, revision_id=42, limit=None)

        assert len(results) == 2
        assert dict(respx_mock.calls.last.request.url.params)["cursor"] == "cursor1"

    def test_list_filtered_by_properties(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        three_d_node: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes/list")
        respx_mock.post(url).respond(status_code=200, json={"items": [three_d_node], "nextCursor": "next"})

        page = toolkit_client.tool.three_d.nodes.paginate_filtered(
            model_id=37,
            revision_id=42,
            filter=ThreeDNodePropertyFilter(properties={"PDMS": {"Area": ["AB76"], "Type": ["PIPE"]}}),
            partition="1/10",
            limit=25,
        )

        assert json.loads(respx_mock.calls.last.request.content) == {
            "filter": {"properties": {"PDMS": {"Area": ["AB76"], "Type": ["PIPE"]}}},
            "partition": "1/10",
            "limit": 25,
        }
        assert page.next_cursor == "next"
        assert page.items[0].model_id == 37
        assert page.items[0].revision_id == 42

    def test_list_filtered_by_name(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        three_d_node: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes/list")
        respx_mock.post(url).respond(status_code=200, json={"items": [three_d_node]})

        toolkit_client.tool.three_d.nodes.list_filtered(
            model_id=37,
            revision_id=42,
            filter=ThreeDNodeNameFilter(names=["PIPE-9MM-HG", "VALVE-HL3"]),
        )

        assert json.loads(respx_mock.calls.last.request.content) == {
            "filter": {"names": ["PIPE-9MM-HG", "VALVE-HL3"]},
            "limit": 100,
        }

    def test_list_ancestors(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        three_d_node: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        url = toolkit_config.create_api_url("/3d/models/37/revisions/42/nodes/1000/ancestors")
        respx_mock.get(url).respond(status_code=200, json={"items": [three_d_node]})

        responses = toolkit_client.tool.three_d.nodes.list_ancestors(
            model_id=37, revision_id=42, node_id=1000, limit=10
        )

        assert dict(respx_mock.calls.last.request.url.params) == {"limit": "10"}
        assert responses[0].dump() == three_d_node
        assert responses[0].model_id == 37
        assert responses[0].revision_id == 42
