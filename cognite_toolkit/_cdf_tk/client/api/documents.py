import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.documents import (
    DocumentAggregateCountItem,
    DocumentAILanguage,
    DocumentAnswer,
    DocumentElement,
    DocumentElementsResponse,
    DocumentPassage,
    DocumentPropertyPath,
    DocumentResponse,
    DocumentSearchHit,
    DocumentStatusResponse,
    DocumentSummary,
    DocumentTemporaryLink,
    DocumentUniqueBucket,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

_SOURCE_FILE_METADATA: tuple[str, str] = ("sourceFile", "metadata")
_UNIQUE_AGGREGATE_LIMIT_MAX = 10_000
_SEARCH_HIGHLIGHT_MAX_LIMIT = 20
_PASSAGES_SEARCH_MAX_LIMIT = 10
_STATUS_ITEM_LIMIT = 1000
_ASK_ITEM_LIMIT = 100
_SUMMARIZE_ITEM_LIMIT = 1


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
        sort: builtins.list[dict[str, Any]] | None,
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
        sort: builtins.list[dict[str, Any]] | None = None,
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
    ) -> builtins.list[DocumentUniqueBucket]:
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

    def _post(
        self, path: str, body: dict[str, Any], accept: str = "application/json", api_version: str | None = None
    ) -> SuccessResponse:
        request = RequestMessage(
            endpoint_url=self._make_url(path),
            method="POST",
            body_content=body,
            accept=accept,
            disable_gzip=self._disable_gzip,
            api_version=api_version,
        )
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def _get(self, path: str, accept: str) -> SuccessResponse:
        request = RequestMessage(endpoint_url=self._make_url(path), method="GET", accept=accept)
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def content(self, item: InternalId | ExternalId | InstanceId) -> str:
        """Retrieve the extracted plain text of a document.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsContentPost
        """
        return self._post("/documents/content", item.dump(), accept="text/plain").body

    def elements(
        self,
        item: InternalId | ExternalId | InstanceId,
        granularity: Literal["words", "lines", "elements"] = "words",
    ) -> builtins.list[DocumentElement]:
        """Retrieve the parsed content of a document as titles, paragraphs, lists, and tables (alpha).

        Args:
            item: The document.
            granularity: Whether text is returned per word, per line, or per element.

        Returns:
            The elements of the document, in reading order.
        """
        response = self._post("/documents/elements", {**item.dump(), "granularity": granularity}, api_version="alpha")
        return DocumentElementsResponse.model_validate_json(response.body).elements

    def search_passages(
        self,
        filter: dict[str, JsonValue],
        limit: int = 10,
        chunk_count: int = 1,
    ) -> builtins.list[DocumentPassage]:
        """Semantic search for passages of documents.

        Args:
            filter: Passage filter, typically with a semanticSearch leaf, for example
                {"semanticSearch": {"property": ["content"], "value": "pump maintenance"}}.
            limit: Maximum number of passages to return, at most 10.
            chunk_count: Expand each passage with this many neighbouring chunks on each side, 1 to 4.

        Returns:
            The matching passages.

        See https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsPassagesSearch
        """
        if not 0 < limit <= _PASSAGES_SEARCH_MAX_LIMIT:
            raise ValueError(f"Limit must be between 1 and {_PASSAGES_SEARCH_MAX_LIMIT}, got {limit}.")
        body: dict[str, Any] = {
            "filter": filter,
            # The API requires an expansion strategy, and its keys are snake_case.
            "expansionStrategy": {"strategy": "symmetric", "chunk_count": chunk_count},
            "limit": limit,
        }
        response = self._post("/documents/passages/search", body)
        return ResponseItems[DocumentPassage].model_validate_json(response.body).items

    def status(
        self, items: Sequence[InternalId | ExternalId | InstanceId], ignore_unknown_ids: bool = False
    ) -> builtins.list[DocumentStatusResponse]:
        """Retrieve how far the processing of documents has come, per kind of derived content (beta).

        Args:
            items: The documents.
            ignore_unknown_ids: Whether to skip documents that do not exist.

        Returns:
            The processing status of each document.
        """
        results: builtins.list[DocumentStatusResponse] = []
        for chunk in chunker_sequence(items, _STATUS_ITEM_LIMIT):
            body = {"items": [item.dump() for item in chunk], "ignoreUnknownIds": ignore_unknown_ids}
            response = self._post("/documents/status", body, api_version="beta")
            results.extend(ResponseItems[DocumentStatusResponse].model_validate_json(response.body).items)
        return results

    def ask(
        self,
        question: str,
        items: Sequence[InternalId | ExternalId | InstanceId],
        language: DocumentAILanguage | None = None,
        additional_context: str | None = None,
        ignore_unknown_ids: bool = False,
    ) -> DocumentAnswer:
        """Ask a question about one or more PDF documents using a language model.

        Args:
            question: The question to ask, 1 to 2048 characters.
            items: The documents, at most 100.
            language: The language of the answer.
            additional_context: Context the model can use to improve its answer, 10 to 2048 characters.
            ignore_unknown_ids: Whether to answer from the available documents when some are missing or
                not fully processed.

        Returns:
            The answer, in parts with references to the document locations they are sourced from.

        See https://api-docs.cognite.com/20230101/tag/Document-AI/operation/document_questioning_ai_tools_documents_ask_post
        """
        if not 0 < len(items) <= _ASK_ITEM_LIMIT:
            raise ValueError(f"Number of documents must be between 1 and {_ASK_ITEM_LIMIT}, got {len(items)}.")
        body: dict[str, Any] = {
            "question": question,
            "fileIds": [item.dump() for item in items],
            "ignoreUnknownIds": ignore_unknown_ids,
        }
        if language is not None:
            body["language"] = language
        if additional_context is not None:
            body["additionalContext"] = additional_context
        response = self._post("/ai/tools/documents/ask", body)
        return DocumentAnswer.model_validate_json(response.body)

    def summarize(
        self,
        items: Sequence[InternalId | ExternalId | InstanceId],
        language: DocumentAILanguage | None = None,
        ignore_unknown_ids: bool = False,
    ) -> builtins.list[DocumentSummary]:
        """Summarize documents using a language model. The API takes one document per request.

        Args:
            items: The documents.
            language: The language of the summaries.
            ignore_unknown_ids: Whether to skip documents that do not exist.

        Returns:
            The summary of each document.

        See https://api-docs.cognite.com/20230101/tag/Document-AI/operation/documents_summary_ai_tools_documents_summarize_post
        """
        results: builtins.list[DocumentSummary] = []
        for chunk in chunker_sequence(items, _SUMMARIZE_ITEM_LIMIT):
            body: dict[str, Any] = {"items": [item.dump() for item in chunk], "ignoreUnknownIds": ignore_unknown_ids}
            if language is not None:
                body["language"] = language
            response = self._post("/ai/tools/documents/summarize", body)
            results.extend(ResponseItems[DocumentSummary].model_validate_json(response.body).items)
        return results

    def preview_image(self, document_id: int, page_number: int = 1) -> bytes:
        """Retrieve a PNG preview of a page of a document. Previews exist for the first 10 pages.

        See https://api-docs.cognite.com/20230101/tag/Document-preview/operation/documentsPreviewImagePage
        """
        return self._get(f"/documents/{document_id}/preview/image/pages/{page_number}", accept="image/png").content

    def preview_pdf(self, document_id: int) -> bytes:
        """Retrieve a PDF preview of a document.

        See https://api-docs.cognite.com/20230101/tag/Document-preview/operation/documentsPreviewPdf
        """
        return self._get(f"/documents/{document_id}/preview/pdf", accept="application/pdf").content

    def preview_pdf_temporary_link(self, document_id: int) -> DocumentTemporaryLink:
        """Retrieve a temporary link to a PDF preview of a document.

        See https://api-docs.cognite.com/20230101/tag/Document-preview/operation/documentsPreviewPdfTemporaryLink
        """
        response = self._get(f"/documents/{document_id}/preview/pdf/temporarylink", accept="application/json")
        return DocumentTemporaryLink.model_validate_json(response.body)

    def paginate(
        self,
        filter: dict[str, Any] | None = None,
        sort: builtins.list[dict[str, Any]] | None = None,
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
        sort: builtins.list[dict[str, Any]] | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[DocumentResponse]]:
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
        sort: builtins.list[dict[str, Any]] | None = None,
        limit: int | None = 100,
    ) -> builtins.list[DocumentResponse]:
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
