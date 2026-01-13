from collections.abc import Sequence
from typing import Literal

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.hosted_extractor_source import (
    HostedExtractorSourceRequestUnion,
    HostedExtractorSourceResponseUnion,
)
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, SuccessResponse2


class HostedExtractorSourcesAPI(
    CDFResourceAPI[ExternalId, HostedExtractorSourceRequestUnion, HostedExtractorSourceResponseUnion]
):
    _response_adapter: TypeAdapter[list[HostedExtractorSourceResponseUnion]] = TypeAdapter(
        list[HostedExtractorSourceResponseUnion]
    )

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(
                    method="POST", path="/hostedextractors/sources", item_limit=100, concurrency_max_workers=1
                ),
                "retrieve": Endpoint(
                    method="POST", path="/hostedextractors/sources/retrieve", item_limit=100, concurrency_max_workers=1
                ),
                "update": Endpoint(
                    method="POST", path="/hostedextractors/sources/update", item_limit=100, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/hostedextractors/sources/delete", item_limit=100, concurrency_max_workers=1
                ),
                "list": Endpoint(method="GET", path="/hostedextractors/sources", item_limit=100),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[HostedExtractorSourceResponseUnion]:
        data = response.json()
        return PagedResponse[HostedExtractorSourceResponseUnion](
            items=self._response_adapter.validate_python(data.get("items", [])),
            nextCursor=data.get("nextCursor"),
        )

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[HostedExtractorSourceRequestUnion]) -> list[HostedExtractorSourceResponseUnion]:
        """Create hosted extractor sources in CDF.

        Args:
            items: List of source request objects to create.
        Returns:
            List of created source response objects.
        """
        return self._request_item_response(list(items), "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[HostedExtractorSourceResponseUnion]:
        """Retrieve hosted extractor sources from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved source response objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[HostedExtractorSourceRequestUnion], mode: Literal["patch", "replace"] = "replace"
    ) -> list[HostedExtractorSourceResponseUnion]:
        """Update hosted extractor sources in CDF.

        Args:
            items: List of source request objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated source response objects.
        """
        return self._update(list(items), mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete hosted extractor sources from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[HostedExtractorSourceResponseUnion]:
        """Iterate over hosted extractor sources in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of source response objects.
        """
        return self._iterate(cursor=cursor, limit=limit)

    def list(self, limit: int | None = 100) -> list[HostedExtractorSourceResponseUnion]:
        """List all hosted extractor sources in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of source response objects.
        """
        return self._list(limit=limit)
