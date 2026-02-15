from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline_config import (
    ExtractionPipelineConfigRequest,
    ExtractionPipelineConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExtractionPipelineConfigId


class ExtractionPipelineConfigsAPI(
    CDFResourceAPI[ExtractionPipelineConfigId, ExtractionPipelineConfigRequest, ExtractionPipelineConfigResponse]
):
    """API for managing extraction pipeline configuration revisions."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/extpipes/config", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/extpipes/config", item_limit=1),
                "delete": Endpoint(method="POST", path="/extpipes/config/revert", item_limit=1),
                "list": Endpoint(method="GET", path="/extpipes/config/revisions", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractionPipelineConfigResponse]:
        result = ExtractionPipelineConfigResponse.model_validate_json(response.body)
        return PagedResponse[ExtractionPipelineConfigResponse](items=[result], nextCursor=None)

    def create(self, items: Sequence[ExtractionPipelineConfigRequest]) -> list[ExtractionPipelineConfigResponse]:
        """Create new configuration revisions for extraction pipelines.

        The CDF API creates one revision at a time, so items are created sequentially.

        Args:
            items: List of configuration revisions to create.

        Returns:
            List of created configuration revisions.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExtractionPipelineConfigId]) -> list[ExtractionPipelineConfigResponse]:
        """Retrieve configuration revisions by their identifiers.

        Each identifier specifies an extraction pipeline external ID and a revision number.

        Args:
            items: List of ExtractionPipelineConfigId objects to retrieve.

        Returns:
            List of retrieved configuration revisions.
        """
        return self._request_item_response(items, "retrieve")

    def delete(self, items: Sequence[ExtractionPipelineConfigId]) -> list[ExtractionPipelineConfigResponse]:
        """Delete configuration revisions. This is called revert in the CDF API, as it reverts
         the configuration to a previous revision.

        Args:
            items: List of ExtractionPipelineConfigId objects to delete.

        Returns:
            List of the latest configuration revisions after the revert.

        """
        return self._request_item_response(items, "delete")

    def paginate(
        self,
        external_id: str,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ExtractionPipelineConfigResponse]:
        """Retrieve a page of configuration revisions for an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            A paged response containing configuration revisions.
        """
        return self._paginate(
            limit=limit,
            cursor=cursor,
            params={"externalId": external_id},
        )

    def iterate(
        self,
        external_id: str,
        limit: int | None = None,
    ) -> Iterable[list[ExtractionPipelineConfigResponse]]:
        """Iterate over all configuration revisions for an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            limit: Maximum number of items to return. None for all items.

        Yields:
            Lists of configuration revisions per page.
        """
        return self._iterate(
            limit=limit,
            params={"externalId": external_id},
        )

    def list(
        self,
        external_id: str,
        limit: int | None = None,
    ) -> list[ExtractionPipelineConfigResponse]:
        """List all configuration revisions for an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of configuration revisions.
        """
        return self._list(
            limit=limit,
            params={"externalId": external_id},
        )
