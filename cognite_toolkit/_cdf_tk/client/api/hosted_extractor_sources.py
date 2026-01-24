from collections.abc import Iterable, Sequence
from typing import Any, Literal

from pydantic import JsonValue, TypeAdapter

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    HostedExtractorSourceRequestUnion,
    HostedExtractorSourceResponse,
    HostedExtractorSourceResponseUnion,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class HostedExtractorSourcesAPI(
    CDFResourceAPI[ExternalId, HostedExtractorSourceRequestUnion, HostedExtractorSourceResponseUnion]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/hostedextractors/sources", item_limit=10),
                "retrieve": Endpoint(method="POST", path="/hostedextractors/sources/byids", item_limit=100),
                "update": Endpoint(method="POST", path="/hostedextractors/sources/update", item_limit=10),
                "delete": Endpoint(method="POST", path="/hostedextractors/sources/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/hostedextractors/sources", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[HostedExtractorSourceResponseUnion]:
        if isinstance(response, SuccessResponse):
            data = response.body_json
        else:
            data = TypeAdapter(dict[str, JsonValue]).validate_json(response.body)
        items = [HostedExtractorSourceResponse.validate_python(item) for item in data.get("items", [])]
        return PagedResponse[HostedExtractorSourceResponseUnion](items=items, nextCursor=data.get("nextCursor"))

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
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

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False, force: bool | None = None) -> None:
        """Delete hosted extractor sources from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
            force: Delete any jobs associated with each item.
        """
        extra_body: dict[str, Any] = {"ignoreUnknownIds": ignore_unknown_ids}
        if force is not None:
            extra_body["force"] = force
        self._request_no_response(items, "delete", extra_body=extra_body)

    def paginate(
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
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[HostedExtractorSourceResponseUnion]]:
        """Iterate over hosted extractor sources in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of source response objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[HostedExtractorSourceResponseUnion]:
        """List all hosted extractor sources in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of source response objects.
        """
        return self._list(limit=limit)
