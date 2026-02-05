from cognite_toolkit._cdf_tk.client.api.instances import WrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedNodeIdentifier,
)
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import (
    RESOURCE_VIEW_MAPPING_SPACE,
    ResourceViewMappingRequest,
    ResourceViewMappingResponse,
)


class ResourceViewMappingsAPI(
    WrappedInstancesAPI[TypedNodeIdentifier, ResourceViewMappingRequest, ResourceViewMappingResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, ResourceViewMappingRequest.VIEW_ID)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[TypedNodeIdentifier]:
        return ResponseItems[TypedNodeIdentifier].model_validate_json(response.body)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ResourceViewMappingResponse]:
        return PagedResponse[ResourceViewMappingResponse].model_validate_json(response.body)

    def list(self, limit: int | None = 100) -> list[ResourceViewMappingResponse]:
        return super()._list_instances(spaces=[RESOURCE_VIEW_MAPPING_SPACE], instance_type="node", limit=limit)
