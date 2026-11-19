"""Annotations API for managing CDF annotations.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Annotations/operation/annotationsCreate
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import AnnotationFilter
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import (
    AnnotatedResourceReference,
    AnnotationRequest,
    AnnotationResponse,
    AnnotationStatus,
    AnnotationType,
)


class AnnotationsAPI(CDFResourceAPI[AnnotationResponse]):
    """API for managing CDF annotations."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/annotations", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/annotations/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/annotations/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/annotations/delete", item_limit=1000),
                "list": Endpoint(method="POST", path="/annotations/list", item_limit=1000),
            },
        )
        self._suggest = Endpoint(method="POST", path="/annotations/suggest", item_limit=1000)
        self._reverse_lookup = Endpoint(method="POST", path="/annotations/reverselookup", item_limit=1000)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[AnnotationResponse]:
        return PagedResponse[AnnotationResponse].model_validate_json(response.body)

    def create(self, items: Sequence[AnnotationRequest]) -> builtins.list[AnnotationResponse]:
        """Create annotations in CDF.

        Args:
            items: List of AnnotationRequest objects to create.

        Returns:
            List of created AnnotationResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId]) -> builtins.list[AnnotationResponse]:
        """Retrieve annotations from CDF by ID.

        Args:
            items: List of InternalId objects to retrieve.

        Returns:
            List of retrieved AnnotationResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def update(
        self, items: Sequence[AnnotationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[AnnotationResponse]:
        """Update annotations in CDF.

        Args:
            items: List of AnnotationRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated AnnotationResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete annotations from CDF.

        Args:
            items: List of InternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def suggest(self, items: Sequence[AnnotationRequest]) -> builtins.list[AnnotationResponse]:
        """Suggest annotations, that is, create them with the status 'suggested'.

        This only requires the annotationsAcl:SUGGEST capability. The status of the items is ignored.

        Args:
            items: The annotations to suggest.

        Returns:
            The created annotations.
        """
        results: builtins.list[AnnotationResponse] = []
        for response in self._chunk_requests(
            items,
            "create",
            lambda chunk: [{k: v for k, v in item.dump().items() if k != "status"} for item in chunk],
            endpoint_path=self._suggest.path,
        ):
            results.extend(self._validate_page_response(response).items)
        return results

    def reverse_lookup(
        self,
        annotated_resource_type: Literal["file", "threedmodel"],
        *,
        annotation_type: AnnotationType | None = None,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
        creating_user: str | None = None,
        status: AnnotationStatus | None = None,
        data: dict[str, JsonValue] | None = None,
        limit: int | None = 100,
    ) -> builtins.list[AnnotatedResourceReference]:
        """Find the annotated resources that have annotations matching a filter.

        For example, find the files with annotations that link to an asset. This is a beta endpoint.

        Args:
            annotated_resource_type: The type of the annotated resources to return.
            annotation_type: Only match annotations of this type.
            creating_app: Only match annotations created by this app.
            creating_app_version: Only match annotations created by this app version.
            creating_user: Only match annotations created by this user.
            status: Only match annotations with this status.
            data: Only match annotations whose data contains these keys and values (case-sensitive).
            limit: Maximum number of resources to return. None returns all.

        Returns:
            The IDs of the matching annotated resources.
        """
        if limit is not None and limit <= 0:
            raise ValueError(f"Limit must be greater than 0, got {limit}.")
        filter_ = self._filter_out_none_values(
            {
                "annotatedResourceType": annotated_resource_type,
                "annotationType": annotation_type,
                "creatingApp": creating_app,
                "creatingAppVersion": creating_app_version,
                "creatingUser": creating_user,
                "status": status,
                "data": data,
            }
        )
        results: builtins.list[AnnotatedResourceReference] = []
        cursor: str | None = None
        while True:
            page_limit = self._reverse_lookup.item_limit
            if limit is not None:
                page_limit = min(limit - len(results), page_limit)
            body: dict[str, Any] = {"filter": filter_, "limit": page_limit}
            if cursor is not None:
                body["cursor"] = cursor
            request = RequestMessage(
                endpoint_url=self._make_url(self._reverse_lookup.path),
                method=self._reverse_lookup.method,
                body_content=body,
                api_version="beta",
            )
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            page = PagedResponse[AnnotatedResourceReference].model_validate_json(response.body)
            results.extend(page.items)
            cursor = page.next_cursor
            if cursor is None or not page.items or (limit is not None and len(results) >= limit):
                return results

    def paginate(
        self,
        filter: AnnotationFilter,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AnnotationResponse]:
        """Get a page of annotations from CDF.

        Args:
            filter: AnnotationFilter to filter annotations.
            limit: Maximum number of annotations to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of AnnotationResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body={"filter": filter.dump()})

    def iterate(
        self,
        filter: AnnotationFilter,
        limit: int | None = None,
    ) -> Iterable[builtins.list[AnnotationResponse]]:
        """Iterate over all annotations in CDF.

        Args:
            filter: AnnotationFilter to filter annotations.
            limit: Maximum total number of annotations to return.

        Returns:
            Iterable of lists of AnnotationResponse objects.
        """
        return self._iterate(limit=limit, body={"filter": filter.dump()})

    def list(
        self,
        filter: AnnotationFilter,
        limit: int | None = None,
    ) -> builtins.list[AnnotationResponse]:
        """List all annotations in CDF.

        Args:
            filter: AnnotationFilter to filter annotations.
            limit: Maximum total number of annotations to return.

        Returns:
            List of AnnotationResponse objects.
        """
        return self._list(limit=limit, body={"filter": filter.dump()})
