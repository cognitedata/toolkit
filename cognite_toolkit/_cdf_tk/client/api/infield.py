from collections.abc import Sequence
from typing import Any

from rich.console import Console

from cognite_toolkit._cdf_tk.client.api.instances import MultiWrappedInstancesAPI, WrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse, QueryResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    DataExplorationConfig,
    InFieldCDMLocationConfigRequest,
    InFieldCDMLocationConfigResponse,
    InFieldLocationConfig,
    InFieldLocationConfigRequest,
    InFieldLocationConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedInstanceIdentifier,
    TypedNodeIdentifier,
)
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse, APMConfigRequest


class InfieldConfigAPI(MultiWrappedInstancesAPI[InFieldLocationConfigRequest, InFieldLocationConfigResponse]):
    _LOCATION_REF = "locationConfig"
    _EXPLORATION_REF = "dataExplorationConfig"

    def __init__(self, http_client: HTTPClient) -> None:
        # 500 is chosen as 1000 is the maximum for nodes, and each location config consists of 1 or 2 nodes
        super().__init__(http_client, query_chunk=500)

    def _retrieve_query(self, items: Sequence[TypedInstanceIdentifier]) -> dict[str, Any]:
        return {
            "with": {
                self._LOCATION_REF: {
                    "limit": len(items),
                    "nodes": {
                        "filter": {
                            "instanceReferences": [
                                {"space": item.space, "externalId": item.external_id} for item in items
                            ]
                        },
                    },
                },
                self._EXPLORATION_REF: {
                    "nodes": {
                        "from": "locationConfig",
                        "direction": "outwards",
                        "through": {
                            "source": InFieldLocationConfig.VIEW_ID.dump(),
                            "identifier": "dataExplorationConfig",
                        },
                    }
                },
            },
            "select": {
                self._LOCATION_REF: {
                    "sources": [{"source": InFieldLocationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                },
                self._EXPLORATION_REF: {
                    "sources": [{"source": DataExplorationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                },
            },
        }

    def _validate_query_response(self, query_response: QueryResponse) -> list[InFieldLocationConfigResponse]:
        exploration_config_results = (
            DataExplorationConfig.model_validate(item) for item in query_response.items.get(self._EXPLORATION_REF, [])
        )
        exploration_config_map = {(item.space, item.external_id): item for item in exploration_config_results}
        results: list[InFieldLocationConfigResponse] = []
        for item in query_response.items.get(self._LOCATION_REF, []):
            location_config = InFieldLocationConfigResponse.model_validate(item)
            exploration_config = location_config.data_exploration_config
            if exploration_config is not None:
                location_config.data_exploration_config = exploration_config_map.get(
                    (exploration_config.space, exploration_config.external_id), exploration_config
                )
            results.append(location_config)
        return results


class InFieldCDMConfigAPI(
    WrappedInstancesAPI[TypedNodeIdentifier, InFieldCDMLocationConfigRequest, InFieldCDMLocationConfigResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, InFieldCDMLocationConfigRequest.VIEW_ID)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[TypedNodeIdentifier]:
        return ResponseItems[TypedNodeIdentifier].model_validate_json(response.body)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[InFieldCDMLocationConfigResponse]:
        return PagedResponse[InFieldCDMLocationConfigResponse].model_validate_json(response.body)


class APMConfigAPI(WrappedInstancesAPI[TypedNodeIdentifier, APMConfigRequest, APMConfigResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, APMConfigRequest.VIEW_ID)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[TypedNodeIdentifier]:
        return ResponseItems[TypedNodeIdentifier].model_validate_json(response.body)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[APMConfigResponse]:
        return PagedResponse[APMConfigResponse].model_validate_json(response.body)


class InfieldAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self.apm_config = APMConfigAPI(http_client)
        self.config = InfieldConfigAPI(http_client)
        self.cdm_config = InFieldCDMConfigAPI(http_client)
