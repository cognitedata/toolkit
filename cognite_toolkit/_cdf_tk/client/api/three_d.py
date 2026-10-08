import builtins
import json
from collections.abc import Iterable, Sequence
from functools import partial
from typing import Any, Literal, TypeVar
from urllib.parse import quote

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import InternalId, ThreeDModelRevisionId
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    ThreeDAssetMappingFilter,
    ThreeDNodeNameFilter,
    ThreeDNodePropertyFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequestId,
    AssetMappingClassicResponse,
    AssetMappingDMRequestId,
    AssetMappingDMResponse,
    ThreeDModelClassicRequest,
    ThreeDModelClassicResponse,
    ThreeDModelDMSRequest,
    ThreeDNodeResponse,
    ThreeDRevisionClassicRequest,
    ThreeDRevisionClassicResponse,
)


class ThreeDClassicModelsAPI(CDFResourceAPI[ThreeDModelClassicResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/3d/models", item_limit=1000),
                "delete": Endpoint(method="POST", path="/3d/models/delete", item_limit=1000),
                "update": Endpoint(method="POST", path="/3d/models/update", item_limit=1000),
                "retrieve": Endpoint(method="GET", path="/3d/models/{modelId}", item_limit=1000),
                "list": Endpoint(method="GET", path="/3d/models", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ThreeDModelClassicResponse]:
        return PagedResponse[ThreeDModelClassicResponse].model_validate_json(response.body)

    def create(
        self, items: Sequence[ThreeDModelClassicRequest | ThreeDModelDMSRequest]
    ) -> builtins.list[ThreeDModelClassicResponse]:
        """Create 3D models.

        Args:
            items: Classic or data modeling format requests. DMS requests must set space and type.

        Returns:
            The created 3D model(s), including the assigned integer id.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, ids: Sequence[InternalId]) -> builtins.list[ThreeDModelClassicResponse]:
        """Retrieve 3D models by their IDs.

        Args:
            ids (Sequence[int]): The IDs of the 3D models to retrieve.

        Returns:
            list[ThreeDModelClassicResponse]: The retrieved 3D model(s).
        """
        retrieved: builtins.list[ThreeDModelClassicResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for id in ids:
            url = endpoint.path.format(modelId=id.id)
            request = RequestMessage(
                endpoint_url=self._make_url(url),
                method=endpoint.method,
                api_version=self._api_version,
                disable_gzip=self._disable_gzip,
            )
            response = self._http_client.request_single_retries(request)
            result = response.get_success_or_raise(request)
            retrieved.append(ThreeDModelClassicResponse.model_validate_json(result.body))
        return retrieved

    def update(
        self, items: Sequence[ThreeDModelClassicRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[ThreeDModelClassicResponse]:
        """Update 3D models in classic format.

        Args:
            items (Sequence[ThreeDModelClassicRequest]): The 3D model(s) to update.
            mode (Literal["patch", "replace"]): The update mode. "patch" only updates explicitly set fields,
                "replace" replaces all fields.

        Returns:
            list[ThreeDModelClassicResponse]: The updated 3D model(s).
        """
        return self._update(items, mode="replace")

    def delete(self, ids: Sequence[InternalId]) -> None:
        """Delete 3D models by their IDs.

        Args:
            ids (Sequence[int]): The IDs of the 3D models to delete.
        """
        self._request_no_response(ids, "delete")

    @staticmethod
    def _create_list_filter(include_revision_info: bool, published: bool | None) -> dict[str, bool]:
        params = {
            # There is a bug in the API. The parameter includeRevisionInfo is expected to be lower case and not
            # camel case as documented. You get error message: Unrecognized query parameter includeRevisionInfo,
            # did you mean includerevisioninfo?
            "includerevisioninfo": include_revision_info,
        }
        if published is not None:
            params["published"] = published
        return params

    def paginate(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDModelClassicResponse]:
        params = self._create_list_filter(include_revision_info, published)
        return self._paginate(limit=limit, cursor=cursor, params=params)

    def iterate(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int | None = 100,
        cursor: str | None = None,
    ) -> Iterable[builtins.list[ThreeDModelClassicResponse]]:
        params = self._create_list_filter(include_revision_info, published)
        return self._iterate(limit=limit, cursor=cursor, params=params)

    def list(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int | None = 100,
    ) -> builtins.list[ThreeDModelClassicResponse]:
        params = self._create_list_filter(include_revision_info, published)
        return self._list(limit=limit, params=params)


class ThreeDClassicRevisionsAPI(CDFResourceAPI[ThreeDRevisionClassicResponse]):
    ENDPOINT = "/3d/models/{modelId}/revisions"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path=self.ENDPOINT, item_limit=1000),
                "delete": Endpoint(method="POST", path=f"{self.ENDPOINT}/delete", item_limit=1000),
                "update": Endpoint(method="POST", path=f"{self.ENDPOINT}/update", item_limit=1000),
                "list": Endpoint(method="GET", path=self.ENDPOINT, item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ThreeDRevisionClassicResponse]:
        return PagedResponse[ThreeDRevisionClassicResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ThreeDRevisionClassicRequest]) -> builtins.list[ThreeDRevisionClassicResponse]:
        """Create 3D revisions in classic format.

        Items are grouped by model_id and the path is formatted accordingly.

        Args:
            items: The 3D revision(s) to create. Each item must have model_id set.

        Returns:
            The created 3D revision(s).
        """
        results: builtins.list[ThreeDRevisionClassicResponse] = []
        for (model_id,), group in self._group_items_by_text_field(items, "model_id").items():
            path = self.ENDPOINT.format(modelId=model_id)
            result = self._request_item_response(group, "create", endpoint=path)
            for item in result:
                item.model_id = int(model_id)
            results.extend(result)
        return results

    def update(
        self,
        items: Sequence[ThreeDRevisionClassicRequest],
        mode: Literal["patch", "replace"] = "replace",
    ) -> builtins.list[ThreeDRevisionClassicResponse]:
        """Update 3D revisions in classic format.

        Items are grouped by model_id and the path is formatted accordingly.

        Args:
            items: The 3D revision(s) to update. Each item must have id and model_id set.
            mode: The update mode. "patch" only updates explicitly set fields,
                "replace" replaces all fields.

        Returns:
            The updated 3D revision(s).
        """
        results: builtins.list[ThreeDRevisionClassicResponse] = []
        endpoint = self._method_endpoint_map["update"]
        for (model_id,), group in self._group_items_by_text_field(items, "model_id").items():
            path = endpoint.path.format(modelId=model_id)
            for response in self._chunk_requests(
                group, "update", serialization=partial(self._serialize_updates, mode=mode), endpoint_path=path
            ):
                page = self._validate_page_response(response)
                for item in page.items:
                    item.model_id = int(model_id)
                results.extend(page.items)
        return results

    def delete(self, ids: Sequence[ThreeDModelRevisionId]) -> None:
        """Delete 3D revisions by their IDs.

        Args:
            ids: The revisions to delete, identified by a sequence of `ThreeDModelRevisionId` objects.
        """
        endpoint = self._method_endpoint_map["delete"]
        for (model_id,), group in self._group_items_by_text_field(ids, "model_id").items():
            path = endpoint.path.format(modelId=model_id)
            self._request_no_response(group, "delete", endpoint=path)

    @staticmethod
    def _create_list_filter(published: bool | None) -> dict[str, bool]:
        params: dict[str, bool] = {}
        if published is not None:
            params["published"] = published
        return params

    def paginate(
        self,
        model_id: int,
        published: bool | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDRevisionClassicResponse]:
        """Fetch a single page of 3D revisions for a model.

        Args:
            model_id: The ID of the model to list revisions for.
            published: Filter based on published status.
            limit: Maximum number of items to return in the page.
            cursor: Cursor for pagination.

        Returns:
            A page of 3D revisions.
        """
        path = self.ENDPOINT.format(modelId=model_id)
        params = self._create_list_filter(published)
        page = self._paginate(limit=limit, cursor=cursor, params=params, endpoint_path=path)
        for item in page.items:
            item.model_id = model_id
        return page

    def iterate(
        self,
        model_id: int,
        published: bool | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[ThreeDRevisionClassicResponse]]:
        """Iterate over all 3D revisions for a model, handling pagination automatically.

        Args:
            model_id: The ID of the model to list revisions for.
            published: Filter based on published status.
            limit: Maximum number of items per page.

        Yields:
            Batches of 3D revisions.
        """
        path = self.ENDPOINT.format(modelId=model_id)
        params = self._create_list_filter(published)
        for items in self._iterate(limit=limit, params=params, endpoint_path=path):
            for item in items:
                item.model_id = model_id
            yield items

    def list(
        self,
        model_id: int,
        published: bool | None = None,
        limit: int | None = 100,
    ) -> builtins.list[ThreeDRevisionClassicResponse]:
        """List all 3D revisions for a model.

        Args:
            model_id: The ID of the model to list revisions for.
            published: Filter based on published status.
            limit: Maximum total number of items to return. None means no limit.

        Returns:
            All matching 3D revisions.
        """
        path = self.ENDPOINT.format(modelId=model_id)
        params = self._create_list_filter(published)
        items = self._list(limit=limit, params=params, endpoint_path=path)
        for item in items:
            item.model_id = model_id
        return items


T_RequestMapping = TypeVar("T_RequestMapping", bound=AssetMappingClassicRequestId | AssetMappingDMRequestId)


class ThreeDClassicAssetMappingAPI(CDFResourceAPI[AssetMappingClassicResponse]):
    ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/mappings"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                # These endpoints are parameterized, so the paths are templates
                "create": Endpoint(method="POST", path=self.ENDPOINT, item_limit=1000),
                "delete": Endpoint(method="POST", path=f"{self.ENDPOINT}/delete", item_limit=1000),
                "list": Endpoint(method="POST", path=f"{self.ENDPOINT}/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[AssetMappingClassicResponse]:
        return PagedResponse[AssetMappingClassicResponse].model_validate_json(response.body)

    def create(self, mappings: Sequence[AssetMappingClassicRequestId]) -> builtins.list[AssetMappingClassicResponse]:
        """Create 3D asset mappings.

        Args:
            mappings (Sequence[AssetMappingClassicRequestId]):
                The 3D asset mapping(s) to create.

        Returns:
            list[AssetMappingClassicResponse]: The created 3D asset mapping(s).
        """
        results: builtins.list[AssetMappingClassicResponse] = []
        endpoint = self._method_endpoint_map["create"]
        for (model_id, revision_id), group in self._group_items_by_text_field(
            mappings, "model_id", "revision_id"
        ).items():
            path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
            result = self._request_item_response(group, "create", endpoint=path)
            for item in result:
                # We append modelId and revisionId to each item since the API does not return them
                # this is needed to fully populate the AssetMappingResponse data class
                object.__setattr__(item, "model_id", int(model_id))
                object.__setattr__(item, "revision_id", int(revision_id))
            results.extend(result)
        return results

    def delete(self, mappings: Sequence[AssetMappingClassicRequestId]) -> None:
        """Delete 3D asset mappings.

        Args:
            mappings (Sequence[AssetMappingClassicRequestId]):
                The 3D asset mapping(s) to delete.
        """
        endpoint = self._method_endpoint_map["delete"]
        for (model_id, revision_id), group in self._group_items_by_text_field(
            mappings, "model_id", "revision_id"
        ).items():
            path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
            self._request_no_response(group, "delete", endpoint=path)
        return None

    def paginate(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDAssetMappingFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AssetMappingClassicResponse]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
        page = self._paginate(
            limit=limit,
            cursor=cursor,
            body={"filter": filter.dump() if filter else None, "getDmsInstances": False},
            endpoint_path=path,
        )
        # Add modelId and revisionId to items since the API does not return them
        for item in page.items:
            object.__setattr__(item, "model_id", model_id)
            object.__setattr__(item, "revision_id", revision_id)
        return page

    def iterate(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDAssetMappingFilter | None = None,
        limit: int = 100,
    ) -> Iterable[builtins.list[AssetMappingClassicResponse]]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
        for items in self._iterate(
            body={"filter": filter.dump() if filter else None, "getDmsInstances": False},
            limit=limit,
            endpoint_path=path,
        ):
            # Add modelId and revisionId to items since the API does not return them
            for item in items:
                object.__setattr__(item, "model_id", model_id)
                object.__setattr__(item, "revision_id", revision_id)
            yield items

    def list(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDAssetMappingFilter | None = None,
        limit: int | None = 100,
    ) -> builtins.list[AssetMappingClassicResponse]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
        items = self._list(
            body={"filter": filter.dump() if filter else None, "getDmsInstances": False},
            limit=limit,
            endpoint_path=path,
        )
        # Add modelId and revisionId to items since the API does not return them
        for item in items:
            object.__setattr__(item, "model_id", model_id)
            object.__setattr__(item, "revision_id", revision_id)
        return items


class ThreeDDMAssetMappingAPI(CDFResourceAPI[AssetMappingDMResponse]):
    ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/mappings"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                # These endpoints are parameterized, so the paths are templates
                "create": Endpoint(method="POST", path=self.ENDPOINT, item_limit=100),
                "delete": Endpoint(method="POST", path=f"{self.ENDPOINT}/delete", item_limit=100),
                "list": Endpoint(method="POST", path=f"{self.ENDPOINT}/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[AssetMappingDMResponse]:
        return PagedResponse[AssetMappingDMResponse].model_validate_json(response.body)

    def create(
        self, mappings: Sequence[AssetMappingDMRequestId], object_3d_space: str, cad_node_space: str
    ) -> builtins.list[AssetMappingDMResponse]:
        """Create 3D asset mappings in Data Modeling format.

        Args:
            mappings (Sequence[AssetMappingDMRequestId]):
                The 3D asset mapping(s) to create
            object_3d_space (str):
                The instance space where the Cognite3DObject are located.
            cad_node_space (str):
                The instance space where the CogniteCADNode are located.
        Returns:
            list[AssetMappingDMResponse]: The created 3D asset mapping(s).
        """
        results: builtins.list[AssetMappingDMResponse] = []
        for (model_id, revision_id), group in self._group_items_by_text_field(
            mappings, "model_id", "revision_id"
        ).items():
            path = self.ENDPOINT.format(modelId=model_id, revisionId=revision_id)
            result = self._request_item_response(
                group,
                "create",
                endpoint=path,
                extra_body={
                    "dmsContextualizationConfig": {
                        "object3DSpace": object_3d_space,
                        "cadNodeSpace": cad_node_space,
                    }
                },
            )
            for item in result:
                # We append modelId and revisionId to each item since the API does not return them
                # this is needed to fully populate the AssetMappingDMResponse data class
                object.__setattr__(item, "model_id", int(model_id))
                object.__setattr__(item, "revision_id", int(revision_id))
            results.extend(result)
        return results

    def delete(self, mappings: Sequence[AssetMappingDMRequestId], object_3d_space: str, cad_node_space: str) -> None:
        """Delete 3D asset mappings in Data Modeling format.

        Args:
            mappings (Sequence[AssetMappingDMRequestId]):
                The 3D asset mapping(s) to delete.
            object_3d_space (str):
                The instance space where the Cognite3DObject are located.
            cad_node_space (str):
                The instance space where the CogniteCADNode are located.
        """
        endpoint = self._method_endpoint_map["delete"]
        for (model_id, revision_id), group in self._group_items_by_text_field(
            mappings, "model_id", "revision_id"
        ).items():
            path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
            self._request_no_response(
                group,
                "delete",
                endpoint=path,
                extra_body={
                    "dmsContextualizationConfig": {
                        "object3DSpace": object_3d_space,
                        "cadNodeSpace": cad_node_space,
                    }
                },
            )
        return None

    def paginate(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDAssetMappingFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AssetMappingDMResponse]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
        page = self._paginate(
            limit=limit,
            cursor=cursor,
            body={"filter": filter.dump() if filter else None, "getDmsInstances": True},
            endpoint_path=path,
        )
        # Add modelId and revisionId to items since the API does not return them
        for item in page.items:
            object.__setattr__(item, "model_id", model_id)
            object.__setattr__(item, "revision_id", revision_id)
        return page

    def iterate(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDAssetMappingFilter | None = None,
        limit: int = 100,
    ) -> Iterable[builtins.list[AssetMappingDMResponse]]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
        for items in self._iterate(
            body={"filter": filter.dump() if filter else None, "getDmsInstances": True}, limit=limit, endpoint_path=path
        ):
            # Add modelId and revisionId to items since the API does not return them
            for item in items:
                object.__setattr__(item, "model_id", model_id)
                object.__setattr__(item, "revision_id", revision_id)
            yield items

    def list(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDAssetMappingFilter | None = None,
        limit: int | None = 100,
    ) -> builtins.list[AssetMappingDMResponse]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(modelId=model_id, revisionId=revision_id)
        items = self._list(
            body={"filter": filter.dump() if filter else None, "getDmsInstances": True}, limit=limit, endpoint_path=path
        )
        # Add modelId and revisionId to items since the API does not return them
        for item in items:
            object.__setattr__(item, "model_id", model_id)
            object.__setattr__(item, "revision_id", revision_id)
        return items


class ThreeDNodesAPI(CDFResourceAPI[ThreeDNodeResponse]):
    """Read nodes in a 3D model revision.

    The revision must be done before these endpoints succeed. Calling them earlier returns HTTP 400.
    """

    ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/nodes"
    _FILTER = Endpoint(method="POST", path=f"{ENDPOINT}/list", item_limit=1000)

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="POST", path=f"{self.ENDPOINT}/byids", item_limit=1000),
                "list": Endpoint(method="GET", path=self.ENDPOINT, item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ThreeDNodeResponse]:
        return PagedResponse[ThreeDNodeResponse].model_validate_json(response.body)

    @staticmethod
    def _assign_revision(items: Iterable[ThreeDNodeResponse], model_id: int, revision_id: int) -> None:
        for item in items:
            item.model_id = model_id
            item.revision_id = revision_id

    def _nodes_path(self, model_id: int, revision_id: int, *suffix: int | str) -> str:
        path = self.ENDPOINT.format(
            modelId=quote(str(model_id), safe=""),
            revisionId=quote(str(revision_id), safe=""),
        )
        if not suffix:
            return path
        encoded_suffix = "/".join(quote(str(part), safe="") for part in suffix)
        return f"{path}/{encoded_suffix}"

    def _list_params(
        self,
        node_id: int | None,
        depth: int | None,
        sort_by_node_id: bool,
        partition: str | None,
        properties: dict[str, dict[str, str]] | None,
    ) -> dict[str, Any]:
        if partition is not None and not sort_by_node_id:
            raise ValueError("partition can only be used when sort_by_node_id is True.")
        return (
            self._filter_out_none_values(
                {
                    "sortByNodeId": sort_by_node_id,
                    "nodeId": node_id,
                    "depth": depth,
                    "partition": partition,
                    "properties": None if properties is None else json.dumps(properties, separators=(",", ":")),
                }
            )
            or {}
        )

    def retrieve(self, model_id: int, revision_id: int, ids: Sequence[int]) -> builtins.list[ThreeDNodeResponse]:
        """Retrieve nodes by ID.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            ids: Node IDs to retrieve. Requests are sent in batches of 1000.

        Returns:
            The retrieved nodes.
        """
        path = self._nodes_path(model_id, revision_id, "byids")
        items = self._request_item_response(InternalId.from_ids(ids), "retrieve", endpoint=path)
        self._assign_revision(items, model_id, revision_id)
        return items

    def paginate(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        depth: int | None = None,
        sort_by_node_id: bool = False,
        partition: str | None = None,
        properties: dict[str, dict[str, str]] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDNodeResponse]:
        """Fetch one page of nodes in a revision.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            node_id: Root of the subtree to return. Defaults to the revision root.
            depth: How many levels below ``node_id`` to include. Depth 0 is the root node.
            sort_by_node_id: List nodes in ascending node ID order.
            partition: Partition of the result, as ``"M/N"``. Requires ``sort_by_node_id``.
            properties: Exact property match. Only nodes matching every given property are returned.
            limit: Maximum number of nodes in the page.
            cursor: Cursor for pagination.

        Returns:
            One page of nodes.
        """
        page = self._paginate(
            limit=limit,
            cursor=cursor,
            params=self._list_params(node_id, depth, sort_by_node_id, partition, properties),
            endpoint_path=self._nodes_path(model_id, revision_id),
        )
        self._assign_revision(page.items, model_id, revision_id)
        return page

    def iterate(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        depth: int | None = None,
        sort_by_node_id: bool = False,
        partition: str | None = None,
        properties: dict[str, dict[str, str]] | None = None,
        limit: int | None = 100,
        cursor: str | None = None,
    ) -> Iterable[builtins.list[ThreeDNodeResponse]]:
        """Iterate nodes in a revision, following pagination cursors.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            node_id: Root of the subtree to return. Defaults to the revision root.
            depth: How many levels below ``node_id`` to include. Depth 0 is the root node.
            sort_by_node_id: List nodes in ascending node ID order.
            partition: Partition of the result, as ``"M/N"``. Requires ``sort_by_node_id``.
            properties: Exact property match. Only nodes matching every given property are returned.
            limit: Maximum number of nodes to return in total. None returns every node.
            cursor: Cursor to start from.

        Yields:
            Batches of nodes.
        """
        params = self._list_params(node_id, depth, sort_by_node_id, partition, properties)
        for items in self._iterate(
            limit=limit,
            cursor=cursor,
            params=params,
            endpoint_path=self._nodes_path(model_id, revision_id),
        ):
            self._assign_revision(items, model_id, revision_id)
            yield items

    def list(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        depth: int | None = None,
        sort_by_node_id: bool = False,
        partition: str | None = None,
        properties: dict[str, dict[str, str]] | None = None,
        limit: int | None = 100,
    ) -> builtins.list[ThreeDNodeResponse]:
        """List nodes in a revision.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            node_id: Root of the subtree to return. Defaults to the revision root.
            depth: How many levels below ``node_id`` to include. Depth 0 is the root node.
            sort_by_node_id: List nodes in ascending node ID order.
            partition: Partition of the result, as ``"M/N"``. Requires ``sort_by_node_id``.
            properties: Exact property match. Only nodes matching every given property are returned.
            limit: Maximum number of nodes to return. None returns every node.

        Returns:
            The matching nodes.
        """
        items = self._list(
            limit=limit,
            params=self._list_params(node_id, depth, sort_by_node_id, partition, properties),
            endpoint_path=self._nodes_path(model_id, revision_id),
        )
        self._assign_revision(items, model_id, revision_id)
        return items

    def _paginate_filtered(
        self,
        model_id: int,
        revision_id: int,
        node_filter: ThreeDNodeNameFilter | ThreeDNodePropertyFilter | None,
        partition: str | None,
        limit: int,
        cursor: str | None,
    ) -> PagedResponse[ThreeDNodeResponse]:
        if not (0 < limit <= self._FILTER.item_limit):
            raise ValueError(f"Limit must be between 1 and {self._FILTER.item_limit}, got {limit}.")
        body = self._filter_out_none_values(
            {
                "filter": node_filter.dump() if node_filter is not None else None,
                "partition": partition,
                "limit": limit,
                "cursor": cursor,
            }
        )
        request = RequestMessage(
            endpoint_url=self._make_url(self._nodes_path(model_id, revision_id, "list")),
            method=self._FILTER.method,
            body_content=body or {},
            disable_gzip=self._disable_gzip,
            api_version=self._api_version,
        )
        result = self._http_client.request_single_retries(request)
        page = self._validate_page_response(result.get_success_or_raise(request))
        self._assign_revision(page.items, model_id, revision_id)
        return page

    def paginate_filtered(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDNodeNameFilter | ThreeDNodePropertyFilter | None = None,
        partition: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDNodeResponse]:
        """Fetch one page of nodes matching a name or property filter.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            filter: Name or property filter. Omit to page every node through the filter endpoint.
            partition: Partition of the result, as ``"M/N"``.
            limit: Maximum number of nodes in the page.
            cursor: Cursor for pagination.

        Returns:
            One page of nodes.
        """
        return self._paginate_filtered(model_id, revision_id, filter, partition, limit, cursor)

    def iterate_filtered(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDNodeNameFilter | ThreeDNodePropertyFilter | None = None,
        partition: str | None = None,
        limit: int | None = 100,
        cursor: str | None = None,
    ) -> Iterable[builtins.list[ThreeDNodeResponse]]:
        """Iterate nodes matching a name or property filter.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            filter: Name or property filter. Omit to iterate every node through the filter endpoint.
            partition: Partition of the result, as ``"M/N"``.
            limit: Maximum number of nodes to return in total. None returns every match.
            cursor: Cursor to start from.

        Yields:
            Batches of nodes.
        """
        next_cursor = cursor
        total = 0
        while True:
            page_limit = self._FILTER.item_limit if limit is None else min(limit - total, self._FILTER.item_limit)
            page = self._paginate_filtered(model_id, revision_id, filter, partition, page_limit, next_cursor)
            yield page.items
            total += len(page.items)
            if page.next_cursor is None or (limit is not None and total >= limit) or not page.items:
                break
            next_cursor = page.next_cursor

    def list_filtered(
        self,
        model_id: int,
        revision_id: int,
        filter: ThreeDNodeNameFilter | ThreeDNodePropertyFilter | None = None,
        partition: str | None = None,
        limit: int | None = 100,
    ) -> builtins.list[ThreeDNodeResponse]:
        """List nodes matching a name or property filter.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            filter: Name or property filter. Omit to list every node through the filter endpoint.
            partition: Partition of the result, as ``"M/N"``.
            limit: Maximum number of nodes to return. None returns every match.

        Returns:
            The matching nodes.
        """
        return [
            item for batch in self.iterate_filtered(model_id, revision_id, filter, partition, limit) for item in batch
        ]

    def paginate_ancestors(
        self,
        model_id: int,
        revision_id: int,
        node_id: int,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDNodeResponse]:
        """Fetch one page of ancestors of a node, including the node itself.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            node_id: Node to walk upward from.
            limit: Maximum number of nodes in the page.
            cursor: Cursor for pagination.

        Returns:
            One page of ancestor nodes.
        """
        page = self._paginate(
            limit=limit,
            cursor=cursor,
            endpoint_path=self._nodes_path(model_id, revision_id, node_id, "ancestors"),
        )
        self._assign_revision(page.items, model_id, revision_id)
        return page

    def iterate_ancestors(
        self,
        model_id: int,
        revision_id: int,
        node_id: int,
        limit: int | None = 100,
        cursor: str | None = None,
    ) -> Iterable[builtins.list[ThreeDNodeResponse]]:
        """Iterate ancestors of a node, including the node itself.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            node_id: Node to walk upward from.
            limit: Maximum number of nodes to return in total. None returns every ancestor.
            cursor: Cursor to start from.

        Yields:
            Batches of ancestor nodes.
        """
        for items in self._iterate(
            limit=limit,
            cursor=cursor,
            endpoint_path=self._nodes_path(model_id, revision_id, node_id, "ancestors"),
        ):
            self._assign_revision(items, model_id, revision_id)
            yield items

    def list_ancestors(
        self,
        model_id: int,
        revision_id: int,
        node_id: int,
        limit: int | None = 100,
    ) -> builtins.list[ThreeDNodeResponse]:
        """List ancestors of a node, including the node itself.

        Args:
            model_id: Model ID.
            revision_id: Revision ID.
            node_id: Node to walk upward from.
            limit: Maximum number of nodes to return. None returns every ancestor.

        Returns:
            The ancestor nodes.
        """
        items = self._list(
            limit=limit,
            endpoint_path=self._nodes_path(model_id, revision_id, node_id, "ancestors"),
        )
        self._assign_revision(items, model_id, revision_id)
        return items


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.models_classic = ThreeDClassicModelsAPI(http_client)
        self.revisions_classic = ThreeDClassicRevisionsAPI(http_client)
        self.nodes = ThreeDNodesAPI(http_client)
        self.asset_mappings_classic = ThreeDClassicAssetMappingAPI(http_client)
        self.asset_mappings_dm = ThreeDDMAssetMappingAPI(http_client)
