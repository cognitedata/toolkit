from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline_config import (
    ExtractionPipelineConfigRequest,
    ExtractionPipelineConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExtractionPipelineConfigId
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

_BASE_PATH = "/extpipes/config"
_REVISIONS_PATH = f"{_BASE_PATH}/revisions"
_REVERT_PATH = f"{_BASE_PATH}/revert"
_LIST_LIMIT = 100


class ExtractionPipelineConfigsAPI(
    CDFResourceAPI[ExtractionPipelineConfigId, ExtractionPipelineConfigRequest, ExtractionPipelineConfigResponse]
):
    """API for managing extraction pipeline configuration revisions.

    This API does not follow the standard items-based CRUD pattern. Instead:
    - Create and revert operate on single objects.
    - Retrieve retrieves a single configuration revision by query parameters.
    - List returns paginated configuration revisions.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path=_REVISIONS_PATH, item_limit=_LIST_LIMIT),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractionPipelineConfigResponse]:
        return PagedResponse[ExtractionPipelineConfigResponse].model_validate_json(response.body)

    def create(self, item: ExtractionPipelineConfigRequest) -> ExtractionPipelineConfigResponse:
        """Create a new configuration revision for an extraction pipeline.

        Args:
            item: The configuration revision to create.

        Returns:
            The created configuration revision.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(_BASE_PATH),
            method="POST",
            body_content=item.dump(),
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise()
        return ExtractionPipelineConfigResponse.model_validate_json(response.body)

    def retrieve(
        self,
        external_id: str,
        revision: int | None = None,
        active_at_time: int | None = None,
    ) -> ExtractionPipelineConfigResponse:
        """Retrieve a single configuration revision.

        By default, the latest revision is retrieved. Either ``revision`` or
        ``active_at_time`` can be specified to retrieve a specific revision.

        Args:
            external_id: External ID of the extraction pipeline.
            revision: The revision number to retrieve.
            active_at_time: Retrieve the revision that was active at this time
                (milliseconds since epoch).

        Returns:
            The retrieved configuration revision.
        """
        params: dict[str, PrimitiveType] = {"externalId": external_id}
        if revision is not None:
            params["revision"] = revision
        if active_at_time is not None:
            params["activeAtTime"] = active_at_time

        request = RequestMessage(
            endpoint_url=self._make_url(_BASE_PATH),
            method="GET",
            parameters=params,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise()
        return ExtractionPipelineConfigResponse.model_validate_json(response.body)

    def paginate(
        self,
        external_id: str,
        limit: int = _LIST_LIMIT,
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

    def revert(self, item: ExtractionPipelineConfigId) -> ExtractionPipelineConfigResponse:
        """Revert the configuration to a specific revision.

        This creates a new revision with the content of the specified revision.

        Args:
            item: The identifier specifying the extraction pipeline and revision to revert to.

        Returns:
            The newly created configuration revision.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(_REVERT_PATH),
            method="POST",
            body_content=item.dump(),
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise()
        return ExtractionPipelineConfigResponse.model_validate_json(response.body)
