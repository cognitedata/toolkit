from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationRequest, TransformationResponse


class TransformationsAPI(CDFResourceAPI[InternalOrExternalId, TransformationRequest, TransformationResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/transformations", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/transformations/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/transformations/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/transformations/delete", item_limit=1000),
                "list": Endpoint(method="POST", path="/transformations/filter", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[TransformationResponse]:
        return PagedResponse[TransformationResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[TransformationRequest]) -> list[TransformationResponse]:
        """Create transformations in CDF.

        Args:
            items: List of TransformationRequest objects to create.
        Returns:
            List of created TransformationResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False, with_job_details: bool = False
    ) -> list[TransformationResponse]:
        """Retrieve transformations from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
            with_job_details: Whether the transformations will be returned with running job and last created job details.
        Returns:
            List of retrieved TransformationResponse objects.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids, "withJobDetails": with_job_details},
        )

    def update(
        self, items: Sequence[TransformationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[TransformationResponse]:
        """Update transformations in CDF.

        Args:
            items: List of TransformationRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated TransformationResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete transformations from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: ClassicFilter | None = None,
        is_public: bool | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TransformationResponse]:
        """Iterate over all transformations in CDF.

        Args:
            filter: Filter by data set IDs.
            is_public: Filter by public status.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of TransformationResponse objects.
        """
        filter_: dict[str, Any] = filter.dump() if filter else {}
        if is_public is not None:
            filter_["isPublic"] = is_public

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_ or None},
        )

    def iterate(
        self,
        filter: ClassicFilter | None = None,
        is_public: bool | None = None,
        limit: int = 100,
    ) -> Iterable[list[TransformationResponse]]:
        """Iterate over all transformations in CDF.

        Args:
            filter: Filter by data set IDs.
            is_public: Filter by public status.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of TransformationResponse objects.
        """
        filter_: dict[str, Any] = filter.dump() if filter else {}
        if is_public is not None:
            filter_["isPublic"] = is_public

        return self._iterate(
            limit=limit,
            body={"filter": filter_ or None},
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[TransformationResponse]:
        """List all transformations in CDF.

        Returns:
            List of TransformationResponse objects.
        """
        return self._list(limit=limit)
