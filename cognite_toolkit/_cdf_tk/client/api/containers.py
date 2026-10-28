"""Containers API for managing CDF data modeling containers.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Containers/operation/ApplyContainers
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any, TypeVar

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.responses import ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import ContainerFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerConstraintId,
    ContainerId,
    ContainerIndexId,
    ContainerInspectResultItem,
    ContainerPropertyId,
    ContainerRequest,
    ContainerResponse,
)
from cognite_toolkit._cdf_tk.constants import CONTAINER_UPSERT_BATCH_LIMIT
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

T_ContainerSubId = TypeVar("T_ContainerSubId", ContainerConstraintId, ContainerIndexId, ContainerPropertyId)


class ContainersAPI(CDFResourceAPI[ContainerResponse]):
    """API for managing CDF data modeling containers.

    Containers use an apply/upsert pattern for create and update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/models/containers", item_limit=CONTAINER_UPSERT_BATCH_LIMIT),
                "retrieve": Endpoint(method="POST", path="/models/containers/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/models/containers/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/models/containers", item_limit=1000),
                "inspect": Endpoint(method="POST", path="/models/containers/inspect", item_limit=10),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ContainerResponse]:
        return PagedResponse[ContainerResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ContainerRequest]) -> builtins.list[ContainerResponse]:
        """Create (create or update) containers in CDF.

        Args:
            items: List of ContainerRequest objects to apply.

        Returns:
            List of applied ContainerResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def update(self, items: Sequence[ContainerRequest]) -> builtins.list[ContainerResponse]:
        """Update (create or update) containers in CDF.

        Args:
            items: List of ContainerRequest objects to apply.
        Returns:
            List of applied ContainerResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[ContainerId]) -> builtins.list[ContainerResponse]:
        """Retrieve containers from CDF.

        Args:
            items: List of ContainerReference objects to retrieve.

        Returns:
            List of retrieved ContainerResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[ContainerId]) -> None:
        """Delete containers from CDF.

        Args:
            items: List of ContainerReference objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        filter: ContainerFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ContainerResponse]:
        """Get a page of containers from CDF.

        Args:
            filter: ContainerFilter to filter containers.
            limit: Maximum number of containers to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of ContainerResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def iterate(
        self,
        filter: ContainerFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[builtins.list[ContainerResponse]]:
        """Iterate over all containers in CDF.

        Args:
            filter: ContainerFilter to filter containers.
            limit: Maximum total number of containers to return.

        Returns:
            Iterable of lists of ContainerResponse objects.
        """
        return self._iterate(
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def inspect(
        self, items: Sequence[ContainerId], all_versions: bool = True, include_unavailable_views: bool = True
    ) -> builtins.list[ContainerInspectResultItem]:
        """Inspect containers to discover which views reference them.

        Args:
            items: Containers to inspect.
            all_versions: Whether to include all view versions in the result. Defaults to True.
            include_unavailable_views: Whether to include unavailable views in the result. Defaults to True.

        Returns:
            One ContainerInspectResultItem per requested container, with the involved views populated.
        """
        inspection_operations = {
            "involvedViews": {"allVersions": all_versions},
            "totalInvolvedViewCount": {
                "allVersions": all_versions,
                "includeUnavailableViews": include_unavailable_views,
            },
        }
        results: builtins.list[ContainerInspectResultItem] = []
        for response in self._chunk_requests(
            items, "inspect", self._serialize_items, extra_body={"inspectionOperations": inspection_operations}
        ):
            results.extend(ResponseItems[ContainerInspectResultItem].model_validate_json(response.body).items)
        return results

    # Non-standard endpoints for container constraints, indexes, and property constraints.
    # These are not part of the method_endpoint_map as they do not map to a standard APIMethod.
    _DELETE_CONSTRAINTS_PATH = "/models/containers/constraints/delete"
    _DELETE_INDEXES_PATH = "/models/containers/indexes/delete"
    _RETRY_CONSTRAINTS_PATH = "/models/containers/constraints/retry"
    _RETRY_INDEXES_PATH = "/models/containers/indexes/retry"
    _RETRY_PROPERTY_CONSTRAINTS_PATH = "/models/containers/properties/constraints/retry"
    _SUB_RESOURCE_ITEM_LIMIT = 10

    def delete_constraints(self, items: Sequence[ContainerConstraintId]) -> builtins.list[ContainerConstraintId]:
        """Delete constraints from containers.

        Args:
            items: The container constraints to delete.

        Returns:
            The identifiers of the deleted constraints.
        """
        return self._request_container_sub_resource(items, self._DELETE_CONSTRAINTS_PATH, ContainerConstraintId)

    def delete_indexes(self, items: Sequence[ContainerIndexId]) -> builtins.list[ContainerIndexId]:
        """Delete indexes from containers.

        Args:
            items: The container indexes to delete.

        Returns:
            The identifiers of the deleted indexes.
        """
        return self._request_container_sub_resource(items, self._DELETE_INDEXES_PATH, ContainerIndexId)

    def retry_constraints(self, items: Sequence[ContainerConstraintId]) -> builtins.list[ContainerConstraintId]:
        """Retry building failed container constraints.

        Args:
            items: The container constraints to retry.

        Returns:
            The identifiers of the constraints that were retried.
        """
        return self._request_container_sub_resource(items, self._RETRY_CONSTRAINTS_PATH, ContainerConstraintId)

    def retry_indexes(self, items: Sequence[ContainerIndexId]) -> builtins.list[ContainerIndexId]:
        """Retry building failed container indexes.

        Args:
            items: The container indexes to retry.

        Returns:
            The identifiers of the indexes that were retried.
        """
        return self._request_container_sub_resource(items, self._RETRY_INDEXES_PATH, ContainerIndexId)

    def retry_property_constraints(self, items: Sequence[ContainerPropertyId]) -> builtins.list[ContainerPropertyId]:
        """Retry building failed property constraints (for example, maxListSize or maxTextSize) on containers.

        Args:
            items: The container properties to retry the constraints for.

        Returns:
            The identifiers of the properties that were retried.
        """
        return self._request_container_sub_resource(items, self._RETRY_PROPERTY_CONSTRAINTS_PATH, ContainerPropertyId)

    @staticmethod
    def _serialize_container_sub_ids(
        items: Sequence[ContainerConstraintId | ContainerIndexId | ContainerPropertyId],
    ) -> builtins.list[dict[str, Any]]:
        return [
            {"space": item.space, "containerExternalId": item.external_id, "identifier": item.identifier}
            for item in items
        ]

    def _request_container_sub_resource(
        self, items: Sequence[T_ContainerSubId], path: str, id_cls: type[T_ContainerSubId]
    ) -> builtins.list[T_ContainerSubId]:
        results: builtins.list[T_ContainerSubId] = []
        for chunk in chunker_sequence(items, self._SUB_RESOURCE_ITEM_LIMIT):
            request = RequestMessage(
                endpoint_url=self._make_url(path),
                method="POST",
                body_content={"items": self._serialize_container_sub_ids(chunk)},
            )
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            body = response.body_json if response.body else {}
            for raw in body.get("items") or []:
                results.append(
                    id_cls(space=raw["space"], external_id=raw["containerExternalId"], identifier=raw["identifier"])
                )
        return results

    def list(self, filter: ContainerFilter | None = None, limit: int | None = None) -> builtins.list[ContainerResponse]:
        """List all containers in CDF.

        Args:
            filter: ContainerFilter to filter containers.
            limit: Maximum total number of containers to return.

        Returns:
            List of ContainerResponse objects.
        """
        return self._list(limit=limit, params=filter.dump() if filter else None)
