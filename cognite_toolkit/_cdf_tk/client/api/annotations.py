"""Annotations API for managing CDF annotations.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Annotations/operation/annotationsCreate
"""

from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationRequest, AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId


class AnnotationsAPI(CDFResourceAPI[InternalId, AnnotationRequest, AnnotationResponse]):
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

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[AnnotationResponse]:
        return PagedResponse[AnnotationResponse].model_validate_json(response.body)

    def create(self, items: Sequence[AnnotationRequest]) -> list[AnnotationResponse]:
        """Create annotations in CDF.

        Args:
            items: List of AnnotationRequest objects to create.

        Returns:
            List of created AnnotationResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId]) -> list[AnnotationResponse]:
        """Retrieve annotations from CDF by ID.

        Args:
            items: List of InternalId objects to retrieve.

        Returns:
            List of retrieved AnnotationResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def update(
        self, items: Sequence[AnnotationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[AnnotationResponse]:
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

    def paginate(
        self,
        annotated_resource_type: str | None = None,
        annotated_resource_ids: list[int] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AnnotationResponse]:
        """Get a page of annotations from CDF.

        Args:
            annotated_resource_type: Filter by annotated resource type.
            annotated_resource_ids: Filter by annotated resource IDs.
            limit: Maximum number of annotations to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of AnnotationResponse objects.
        """
        body = {}
        if annotated_resource_type is not None:
            body["annotatedResourceType"] = annotated_resource_type
        if annotated_resource_ids is not None:
            body["annotatedResourceIds"] = [{"id": id_} for id_ in annotated_resource_ids]
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=body if body else None,
        )

    def iterate(
        self,
        annotated_resource_type: str | None = None,
        annotated_resource_ids: list[int] | None = None,
        limit: int | None = None,
    ) -> Iterable[list[AnnotationResponse]]:
        """Iterate over all annotations in CDF.

        Args:
            annotated_resource_type: Filter by annotated resource type.
            annotated_resource_ids: Filter by annotated resource IDs.
            limit: Maximum total number of annotations to return.

        Returns:
            Iterable of lists of AnnotationResponse objects.
        """
        body = {}
        if annotated_resource_type is not None:
            body["annotatedResourceType"] = annotated_resource_type
        if annotated_resource_ids is not None:
            body["annotatedResourceIds"] = [{"id": id_} for id_ in annotated_resource_ids]
        return self._iterate(limit=limit, body=body if body else None)

    def list(
        self,
        annotated_resource_type: str | None = None,
        annotated_resource_ids: list[int] | None = None,
        limit: int | None = None,
    ) -> list[AnnotationResponse]:
        """List all annotations in CDF.

        Args:
            annotated_resource_type: Filter by annotated resource type.
            annotated_resource_ids: Filter by annotated resource IDs.
            limit: Maximum total number of annotations to return.

        Returns:
            List of AnnotationResponse objects.
        """
        return [
            item
            for batch in self.iterate(
                annotated_resource_type=annotated_resource_type,
                annotated_resource_ids=annotated_resource_ids,
                limit=limit,
            )
            for item in batch
        ]
