from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline_config import (
    ExtractionPipelineConfigRequest,
    ExtractionPipelineConfigResponse,
)
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

_BASE_PATH = "/extpipes/config"
_REVISIONS_PATH = f"{_BASE_PATH}/revisions"
_REVERT_PATH = f"{_BASE_PATH}/revert"
_LIST_LIMIT = 100


class ExtractionPipelineConfigAPI:
    """API for managing extraction pipeline configuration revisions.

    This API does not follow the standard items-based CRUD pattern. Instead:
    - Create and revert operate on single objects.
    - Get retrieves a single configuration revision by query parameters.
    - List returns paginated configuration revisions.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _make_url(self, path: str) -> str:
        return self._http_client.config.create_api_url(path)

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

    def get(
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
        params: dict[str, PrimitiveType] = {"externalId": external_id, "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor

        request = RequestMessage(
            endpoint_url=self._make_url(_REVISIONS_PATH),
            method="GET",
            parameters=params,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise()
        return PagedResponse[ExtractionPipelineConfigResponse].model_validate_json(response.body)

    def iterate(
        self,
        external_id: str,
        limit: int = _LIST_LIMIT,
    ) -> Iterable[list[ExtractionPipelineConfigResponse]]:
        """Iterate over all configuration revisions for an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            limit: Maximum number of items to return per page.

        Yields:
            Lists of configuration revisions per page.
        """
        cursor: str | None = None
        while True:
            page = self.paginate(external_id=external_id, limit=limit, cursor=cursor)
            yield page.items
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

    def list(
        self,
        external_id: str,
        limit: int | None = _LIST_LIMIT,
    ) -> list[ExtractionPipelineConfigResponse]:
        """List all configuration revisions for an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of configuration revisions.
        """
        all_items: list[ExtractionPipelineConfigResponse] = []
        for batch in self.iterate(external_id=external_id, limit=limit or _LIST_LIMIT):
            all_items.extend(batch)
            if limit is not None and len(all_items) >= limit:
                return all_items[:limit]
        return all_items

    def revert(self, item: ExtractionPipelineConfigRevertRequest) -> ExtractionPipelineConfigResponse:
        """Revert the configuration to a specific revision.

        This creates a new revision with the content of the specified revision.

        Args:
            item: The revert request specifying the extraction pipeline and revision.

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
