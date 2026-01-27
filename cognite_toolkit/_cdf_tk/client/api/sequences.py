from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import SequenceRequest, SequenceResponse


class SequencesAPI(CDFResourceAPI[InternalOrExternalId, SequenceRequest, SequenceResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/sequences", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/sequences/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "update": Endpoint(method="POST", path="/sequences/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/sequences/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/sequences/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SequenceResponse]:
        return PagedResponse[SequenceResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[SequenceRequest]) -> list[SequenceResponse]:
        """Create sequences in CDF.

        Args:
            items: List of SequenceRequest objects to create.
        Returns:
            List of created SequenceResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[SequenceResponse]:
        """Retrieve sequences from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved SequenceResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[SequenceRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[SequenceResponse]:
        """Update sequences in CDF.

        Args:
            items: List of SequenceRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated SequenceResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete sequences from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: ClassicFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SequenceResponse]:
        """Iterate over all sequences in CDF.

        Args:
            filter: Filter by data set IDs and/or asset subtree IDs.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SequenceResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter.dump() if filter else None},
        )

    def iterate(
        self,
        filter: ClassicFilter | None = None,
        limit: int = 100,
    ) -> Iterable[list[SequenceResponse]]:
        """Iterate over all sequences in CDF.

        Args:
            filter: Filter by data set IDs and/or asset subtree IDs.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SequenceResponse objects.
        """
        return self._iterate(
            limit=limit,
            body={"filter": filter.dump() if filter else None},
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[SequenceResponse]:
        """List all sequences in CDF.

        Returns:
            List of SequenceResponse objects.
        """
        return self._list(limit=limit)
