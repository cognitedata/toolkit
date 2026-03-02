from cognite_toolkit._cdf_tk.client.api.instances import WrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import (
    CogniteFileRequest,
    CogniteFileResponse,
)


class CogniteFilesAPI(WrappedInstancesAPI[NodeReference, CogniteFileResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, CogniteFileRequest.VIEW_ID)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[NodeReference]:
        return ResponseItems[NodeReference].model_validate_json(response.body)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[CogniteFileResponse]:
        return PagedResponse[CogniteFileResponse].model_validate_json(response.body)

    def list(self, spaces: list[str] | None = None, limit: int | None = 100) -> list[CogniteFileResponse]:
        """List all CogniteFile instances.

        Args:
            spaces: Optional list of spaces to filter by.
            limit: Maximum number of items to return. If None, all items are returned.

        Returns:
            List of CogniteFileResponse objects.
        """
        return super()._list_instances(spaces=spaces, instance_type="node", limit=limit)
