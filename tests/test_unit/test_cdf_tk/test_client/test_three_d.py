from typing import Any

import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.three_d import AssetMappingClassicRequest


@pytest.fixture()
def asset_mapping_classic_response() -> dict[str, Any]:
    return {
        "items": [
            {
                "nodeId": 123,
                "assetId": 456,
                "treeIndex": 1,
                "subtreeSize": 10,
            }
        ],
    }


@pytest.fixture()
def asset_mapping_three_d_response() -> dict[str, Any]:
    return {
        "items": [
            {
                "nodeId": 123,
                "assetInstanceId": {
                    "space": "my_space",
                    "externalId": "my_external_id",
                },
                "treeIndex": 1,
                "subtreeSize": 10,
            }
        ],
    }


class TestAssetsMappings:
    def test_create(
        self,
        toolkit_config: ToolkitClientConfig,
        asset_mapping_classic_response: dict[str, Any],
        respx_mock: respx.Router,
    ) -> None:
        config = toolkit_config
        url = config.create_api_url("/3d/models/37/revisions/42/mappings")
        respx_mock.post(url).respond(status_code=200, json=asset_mapping_classic_response)
        client = ToolkitClient(config)

        responses = client.tool.three_d.asset_mappings.create(
            [AssetMappingClassicRequest(nodeId=123, assetId=456, modelId=37, revisionId=42)]
        )
        assert len(responses) == 1
        response = responses[0]
        assert response.model_dump(by_alias=True, exclude_unset=True) == asset_mapping_classic_response["items"][0]
        assert response.model_id == 37
        assert response.revision_id == 42
