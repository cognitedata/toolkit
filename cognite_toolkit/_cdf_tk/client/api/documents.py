from collections.abc import Iterable
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.documents import (
    DocumentAggregateCountItem,
    DocumentPropertyPath,
    DocumentResponse,
    DocumentSearchHit,
    DocumentUniqueBucket,
)

_SOURCE_FILE_METADATA: tuple[str, str] = ("sourceFile", "metadata")
_UNIQUE_AGGREGATE_LIMIT_MAX = 10_000
_SEARCH_HIGHLIGHT_MAX_LIMIT = 20


class DocumentsAPI(CDFResourceAPI[DocumentResponse]):
    """Documents API (list, search, and aggregate helpers)."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "aggregate": Endpoint(method="POST", path="/documents/aggregate", item_limit=1000),
                "list": Endpoint(method="POST", path="/documents/list", item_limit=1000),
                "search": Endpoint(method="POST", path="/documents/search", item_limit=1000),
            },
        )

    @staticmethod
    def _documents_list_body(
        filter: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if filter is not None:
            body["filter"] = filter
        if sort is not None:
            body["sort"] = sort
        return body

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DocumentResponse]:
        return PagedResponse[DocumentResponse].model_validate_json(response.body)

    def search(
        self,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        sort: list[dict[str, Any]] | None = None,
        limit: int = 100,
        cursor: str | None = None,
        highlight: bool = False,
    ) -> PagedResponse[DocumentSearchHit]:
        """Search documents with optional full-text query, filter, and sort (``POST /documents/search``).

        When ``highlight`` is true, ``limit`` must be at most 20.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsSearch
        """
        search_endpoint = self._method_endpoint_map["search"]
        if not 0 < limit <= search_endpoint.item_limit:
            raise ValueError(f"Limit must be between 1 and {search_endpoint.item_limit}, got {limit}.")
        if highlight and limit > _SEARCH_HIGHLIGHT_MAX_LIMIT:
            raise ValueError(
                f"When highlight is True, limit must be at most {_SEARCH_HIGHLIGHT_MAX_LIMIT}, got {limit}."
            )

        body: dict[str, Any] = {"limit": limit, "highlight": highlight}
        if cursor is not None:
            body["cursor"] = cursor
        if filter is not None:
            body["filter"] = filter
        if sort is not None:
            body["sort"] = sort
        if query is not None:
            body["search"] = {"query": query}

        req = RequestMessage(
            endpoint_url=self._make_url(search_endpoint.path),
            method=search_endpoint.method,
            body_content=body,
            disable_gzip=self._disable_gzip,
            api_version=self._api_version,
        )
        result = self._http_client.request_single_retries(req).get_success_or_raise(req)
        return PagedResponse[DocumentSearchHit].model_validate_json(result.body)

    def _post_aggregate(self, body: dict[str, Any]) -> SuccessResponse:
        aggregate_endpoint = self._method_endpoint_map["aggregate"]
        req = RequestMessage(
            endpoint_url=self._make_url(aggregate_endpoint.path),
            method=aggregate_endpoint.method,
            body_content=body,
            disable_gzip=self._disable_gzip,
            api_version=self._api_version,
        )
        return self._http_client.request_single_retries(req).get_success_or_raise(req)

    @staticmethod
    def _search_and_filter_body(*, query: str | None, filter: dict[str, Any] | None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if filter is not None:
            body["filter"] = filter
        if query is not None:
            body["search"] = {"query": query}
        return body

    @staticmethod
    def _first_aggregate_count(response: SuccessResponse) -> int:
        items = ResponseItems[DocumentAggregateCountItem].model_validate_json(response.body).items
        return items[0].count if items else 0

    def count(self, *, query: str | None = None, filter: dict[str, Any] | None = None) -> int:
        """Count documents matching optional full-text ``query`` and/or ``filter``.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsAggregate
        """
        body = self._search_and_filter_body(query=query, filter=filter)
        body["aggregate"] = "count"
        return self._first_aggregate_count(self._post_aggregate(body))

    def cardinality(
        self,
        property: DocumentPropertyPath,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
    ) -> int:
        """Approximate number of distinct values (or distinct metadata keys for ``sourceFile``/``metadata``).

        Uses ``cardinalityValues`` for almost all paths, and ``cardinalityProperties`` when
        ``property`` is exactly ``("sourceFile", "metadata")``, per the aggregate API.

        ``property`` must be a path allowed on document search filters; see
        https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsSearch

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsAggregate
        """
        body = self._search_and_filter_body(query=query, filter=filter)
        if property == _SOURCE_FILE_METADATA:
            body["aggregate"] = "cardinalityProperties"
            body["path"] = ["sourceFile", "metadata"]
        else:
            body["aggregate"] = "cardinalityValues"
            body["properties"] = [{"property": list(property)}]
        return self._first_aggregate_count(self._post_aggregate(body))

    def unique(
        self,
        property: DocumentPropertyPath,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 100,
    ) -> list[DocumentUniqueBucket]:
        """Top distinct values for a field, each with a count and normalized ``values`` list.

        Uses ``uniqueValues`` for almost all paths, and ``uniqueProperties`` when ``property`` is
        exactly ``("sourceFile", "metadata")``.

        ``property`` must be a path allowed on document search filters; see
        https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsSearch

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsAggregate
        """
        if not 1 <= limit <= _UNIQUE_AGGREGATE_LIMIT_MAX:
            raise ValueError(f"Limit must be between 1 and {_UNIQUE_AGGREGATE_LIMIT_MAX}, got {limit}.")
        body = self._search_and_filter_body(query=query, filter=filter)
        if property == _SOURCE_FILE_METADATA:
            body["aggregate"] = "uniqueProperties"
            body["properties"] = [{"property": ["sourceFile", "metadata"]}]
        else:
            body["aggregate"] = "uniqueValues"
            body["properties"] = [{"property": list(property)}]
        body["limit"] = limit
        response = self._post_aggregate(body)
        return ResponseItems[DocumentUniqueBucket].model_validate_json(response.body).items

    def paginate(
        self,
        filter: dict[str, Any] | None = None,
        sort: list[dict[str, Any]] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[DocumentResponse]:
        """Fetch one page of documents (``POST /documents/list``).

        Args:
            filter: Document filter expression.
            sort: Sort specification (at most one property, per API).
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of DocumentResponse objects.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsList
        """
        body = self._documents_list_body(filter, sort)
        return self._paginate(limit=limit, cursor=cursor, body=body)

    def iterate(
        self,
        filter: dict[str, Any] | None = None,
        sort: list[dict[str, Any]] | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[DocumentResponse]]:
        """Iterate over document list pages in CDF.

        Args:
            filter: Document filter expression.
            sort: Sort specification (at most one property, per API).
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of DocumentResponse objects (one list per page).

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsList
        """
        body = self._documents_list_body(filter, sort)
        return self._iterate(limit=limit, body=body)

    def list(
        self,
        filter: dict[str, Any] | None = None,
        sort: list[dict[str, Any]] | None = None,
        limit: int | None = 100,
    ) -> list[DocumentResponse]:
        """List documents in CDF.

        Args:
            filter: Document filter expression.
            sort: Sort specification (at most one property, per API).
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of DocumentResponse objects.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsList
        """
        body = self._documents_list_body(filter, sort)
        return self._list(limit=limit, body=body)
