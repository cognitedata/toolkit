import json
from typing import Any

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequest,
    AssetMappingDMRequest,
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
            [AssetMappingClassicRequest(nodeId=123, assetId=456, modelId=37, revisionId=42)]
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
        respx_mock.delete(url).respond(status_code=200, json={})

        toolkit_client.tool.three_d.asset_mappings_classic.delete(
            [AssetMappingClassicRequest(nodeId=123, assetId=456, modelId=37, revisionId=42)]
        )

    @pytest.mark.parametrize(
        "args, expected_error",
        [
            pytest.param({"limit": -10}, "Limit must be between 1 and 1000, got -10.", id="negative limit"),
            pytest.param({"limit": 0}, "Limit must be between 1 and 1000, got 0.", id="zero limit"),
            pytest.param({"limit": 1001}, "Limit must be between 1 and 1000, got 1001.", id="excessive limit"),
            pytest.param(
                {"asset_ids": [1], "node_ids": [2]},
                "Only one of asset_ids, asset_instance_ids, node_ids, or tree_indexes can be provided.",
                id="multiple filters",
            ),
            pytest.param(
                {"asset_ids": []},
                "asset_ids must contain between 1 and 100 IDs.",
                id="empty asset_ids",
            ),
            pytest.param(
                {"asset_ids": list(range(101))},
                "asset_ids must contain between 1 and 100 IDs.",
                id="too many asset_ids",
            ),
            pytest.param(
                {"asset_instance_ids": []},
                "asset_instance_ids must contain between 1 and 100 IDs.",
                id="empty asset_instance_ids",
            ),
            pytest.param(
                {"asset_instance_ids": [f"id_{i}" for i in range(101)]},
                "asset_instance_ids must contain between 1 and 100 IDs.",
                id="too many asset_instance_ids",
            ),
            pytest.param(
                {"node_ids": []},
                "node_ids must contain between 1 and 100 IDs.",
                id="empty node_ids",
            ),
            pytest.param(
                {"node_ids": list(range(101))},
                "node_ids must contain between 1 and 100 IDs.",
                id="too many node_ids",
            ),
            pytest.param(
                {"tree_indexes": []},
                "tree_indexes must contain between 1 and 100 indexes.",
                id="empty tree_indexes",
            ),
            pytest.param(
                {"tree_indexes": list(range(101))},
                "tree_indexes must contain between 1 and 100 indexes.",
                id="too many tree_indexes",
            ),
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
                AssetMappingDMRequest(
                    nodeId=123,
                    assetInstanceId=NodeReference(space="my_space", externalId="my_external_id"),
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
                AssetMappingDMRequest(
                    nodeId=123,
                    assetInstanceId=NodeReference(space="my_space", externalId="my_external_id"),
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

        def callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            items = payload.get("items", [])
            for item in items:
                item["treeIndex"] = 1
                item["subtreeSize"] = 10
            return httpx.Response(status_code=200, json={"items": items})

        respx_mock.post(url_model_37).mock(side_effect=callback)
        respx_mock.post(url_model_38).mock(side_effect=callback)

        mappings = [
            AssetMappingDMRequest(
                nodeId=i,
                assetInstanceId=NodeReference(space="space", externalId=f"external_{i}"),
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
        asset_mapping_classic: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings/list")
        respx_mock.post(url).respond(
            status_code=200,
            json={
                "items": [asset_mapping_classic, asset_mapping_dm],
                "nextCursor": "next",
            },
        )

        page = toolkit_client.tool.three_d.asset_mappings_dm.paginate(model_id=37, revision_id=42, limit=100)
        assert len(page.items) == 2
        assert page.items[0].dump() == asset_mapping_classic
        assert page.items[1].dump() == asset_mapping_dm
        assert page.next_cursor == "next"

    def test_list_with_pagination(
        self,
        toolkit_config: ToolkitClientConfig,
        toolkit_client: ToolkitClient,
        asset_mapping_dm: dict[str, Any],
        asset_mapping_classic: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings/list")
        respx_mock.post(url).side_effect = [
            respx.MockResponse(status_code=200, json={"items": [asset_mapping_classic], "nextCursor": "cursor1"}),
            respx.MockResponse(status_code=200, json={"items": [asset_mapping_dm], "nextCursor": None}),
        ]

        results = toolkit_client.tool.three_d.asset_mappings_dm.list(model_id=37, revision_id=42, limit=None)
        assert len(results) == 2
        assert results[0].dump() == asset_mapping_classic
        assert results[1].dump() == asset_mapping_dm

        assert respx_mock.calls.call_count == 2
