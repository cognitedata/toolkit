from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.documents import (
    DocumentPropertyPath,
    DocumentsApiItem,
    DocumentUniqueBucket,
)

_SOURCE_FILE_METADATA: tuple[str, str] = ("sourceFile", "metadata")
_UNIQUE_AGGREGATE_LIMIT_MAX = 10_000


class DocumentsAPI(CDFResourceAPI[DocumentsApiItem]):
    """Documents API (aggregate helpers and future list support)."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "list": Endpoint(method="POST", path="/documents/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DocumentsApiItem]:
        return PagedResponse[DocumentsApiItem].model_validate_json(response.body)

    def _post_aggregate(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        req = RequestMessage(
            endpoint_url=self._make_url("/documents/aggregate"),
            method="POST",
            body_content=body,
            disable_gzip=self._disable_gzip,
            api_version=self._api_version,
        )
        result = self._http_client.request_single_retries(req).get_success_or_raise(req)
        payload = result.body_json
        items = payload.get("items")
        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]

    @staticmethod
    def _search_and_filter_body(*, query: str | None, filter: dict[str, Any] | None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if filter is not None:
            body["filter"] = filter
        if query is not None:
            body["search"] = {"query": query}
        return body

    @staticmethod
    def _single_count(items: list[dict[str, Any]]) -> int:
        if not items:
            return 0
        raw = items[0].get("count")
        return int(raw) if raw is not None else 0

    @staticmethod
    def _parse_unique_bucket(raw: dict[str, Any]) -> DocumentUniqueBucket:
        count = int(raw["count"])
        if "values" in raw:
            v = raw["values"]
            values = list(v) if isinstance(v, list) else [v]
        elif "value" in raw:
            values = [raw["value"]]
        else:
            values = []
        return DocumentUniqueBucket(count=count, values=values)

    def count(self, *, query: str | None = None, filter: dict[str, Any] | None = None) -> int:
        """Count documents matching optional full-text ``query`` and/or ``filter``.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsAggregate
        """
        body = self._search_and_filter_body(query=query, filter=filter)
        body["aggregate"] = "count"
        return self._single_count(self._post_aggregate(body))

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
        return self._single_count(self._post_aggregate(body))

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
        raw_items = self._post_aggregate(body)
        return [self._parse_unique_bucket(item) for item in raw_items]
