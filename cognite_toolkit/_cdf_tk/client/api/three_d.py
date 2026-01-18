from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any, TypeVar

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsRequest2,
    ItemsSuccessResponse2,
    SuccessResponse2,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequest,
    AssetMappingDMRequest,
    AssetMappingResponse,
    ThreeDModelClassicRequest,
    ThreeDModelResponse,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class ThreeDClassicModelsAPI(CDFResourceAPI[InternalId, ThreeDModelClassicRequest, ThreeDModelResponse]):
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
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[ThreeDModelResponse]:
        return PagedResponse[ThreeDModelResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ThreeDModelClassicRequest]) -> list[ThreeDModelResponse]:
        """Create 3D models in classic format.

        Args:
            items (Sequence[ThreeDModelClassicRequest]): The 3D model(s) to create.

        Returns:
            list[ThreeDModelResponse]: The created 3D model(s).
        """
        return self._request_item_response(items, "create")

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
    ) -> PagedResponse[ThreeDModelResponse]:
        params = self._create_list_filter(include_revision_info, published)
        return self._paginate(limit=limit, cursor=cursor, params=params)

    def iterate(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Iterable[list[ThreeDModelResponse]]:
        params = self._create_list_filter(include_revision_info, published)
        return self._iterate(limit=limit, cursor=cursor, params=params)

    def list(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int | None = 100,
    ) -> list[ThreeDModelResponse]:
        params = self._create_list_filter(include_revision_info, published)
        return self._list(limit=limit, params=params)


T_RequestMapping = TypeVar("T_RequestMapping", bound=AssetMappingClassicRequest | AssetMappingDMRequest)


class ThreeDAssetMappingAPI(
    CDFResourceAPI[AssetMappingClassicRequest, AssetMappingClassicRequest, AssetMappingResponse]
):
    ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/mappings"
    CREATE_CLASSIC_MAX_MAPPINGS_PER_REQUEST = 1000
    CREATE_DM_MAX_MAPPINGS_PER_REQUEST = 100
    DELETE_CLASSIC_MAX_MAPPINGS_PER_REQUEST = 1000
    DELETE_DM_MAX_MAPPINGS_PER_REQUEST = 100

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                # These endpoints are parameterized, so the paths are templates
                "create": Endpoint(method="POST", path=self.ENDPOINT, item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(
                    method="DELETE", path=f"{self.ENDPOINT}/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path=f"{self.ENDPOINT}/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[AssetMappingResponse]:
        return PagedResponse[AssetMappingResponse].model_validate_json(response.body)

    def create(self, mappings: Sequence[AssetMappingClassicRequest]) -> list[AssetMappingResponse]:
        """Create 3D asset mappings.

        Args:
            mappings (Sequence[AssetMappingClassicRequest]):
                The 3D asset mapping(s) to create.

        Returns:
            list[AssetMappingResponse]: The created 3D asset mapping(s).
        """
        results: list[AssetMappingResponse] = []
        for endpoint, model_id, revision_id, revision_mappings in self._chunk_mappings_by_endpoint(
            mappings, self.CREATE_CLASSIC_MAX_MAPPINGS_PER_REQUEST
        ):
            responses = self._http_client.request_items_retries(
                ItemsRequest2(
                    endpoint_url=self._make_url(endpoint),
                    method="POST",
                    items=revision_mappings,
                )
            )
            responses.raise_for_status()
            items = responses.get_items()
            for item in items:
                # We append modelId and revisionId to each item since the API does not return them
                # this is needed to fully populate the AssetMappingResponse data class
                item["modelId"] = model_id
                item["revisionId"] = revision_id
            results.extend(TypeAdapter(list[AssetMappingResponse]).validate_python(items))
        return results

    def create_dm(
        self, mappings: Sequence[AssetMappingDMRequest], object_3d_space: str, cad_node_space: str
    ) -> list[AssetMappingResponse]:
        """Create 3D asset mappings in Data Modeling format.

        Args:
            mappings (Sequence[AssetMappingDMRequest]):
                The 3D asset mapping(s) to create
            object_3d_space (str):
                The instance space where the Cognite3DObject are located.
            cad_node_space (str):
                The instance space where the CogniteCADNode are located.
        Returns:
            list[AssetMappingResponse]: The created 3D asset mapping(s).
        """
        results: list[AssetMappingResponse] = []
        for endpoint, model_id, revision_id, revision_mappings in self._chunk_mappings_by_endpoint(
            mappings, self.CREATE_DM_MAX_MAPPINGS_PER_REQUEST
        ):
            responses = self._http_client.request_items_retries(
                ItemsRequest2(
                    endpoint_url=self._make_url(endpoint),
                    method="POST",
                    items=revision_mappings,
                    extra_body_fields={
                        "dmsContextualizationConfig": {
                            "object3DSpace": object_3d_space,
                            "cadNodeSpace": cad_node_space,
                        }
                    },
                )
            )
            responses.raise_for_status()
            items = responses.get_items()
            for item in items:
                # We append modelId and revisionId to each item since the API does not return them
                # this is needed to fully populate the AssetMappingResponse data class
                item["modelId"] = model_id
                item["revisionId"] = revision_id
            results.extend(TypeAdapter(list[AssetMappingResponse]).validate_python(items))
        return results

    @classmethod
    def _chunk_mappings_by_endpoint(
        cls, mappings: Sequence[T_RequestMapping], chunk_size: int
    ) -> Iterable[tuple[str, int, int, list[T_RequestMapping]]]:
        chunked_mappings: dict[tuple[int, int], list[T_RequestMapping]] = defaultdict(list)
        for mapping in mappings:
            key = mapping.model_id, mapping.revision_id
            chunked_mappings[key].append(mapping)
        for (model_id, revision_id), revision_mappings in chunked_mappings.items():
            endpoint = cls.ENDPOINT.format(modelId=model_id, revisionId=revision_id)
            for chunk in chunker_sequence(revision_mappings, chunk_size):
                yield endpoint, model_id, revision_id, chunk

    def delete(self, mappings: Sequence[AssetMappingClassicRequest]) -> None:
        """Delete 3D asset mappings.

        Args:
            mappings (Sequence[AssetMappingClassicRequest]):
                The 3D asset mapping(s) to delete.
        """
        for endpoint, *_, revision_mappings in self._chunk_mappings_by_endpoint(
            mappings, self.DELETE_CLASSIC_MAX_MAPPINGS_PER_REQUEST
        ):
            responses = self._http_client.request_items_retries(
                ItemsRequest2(
                    endpoint_url=self._make_url(f"{endpoint}/delete"),
                    method="DELETE",
                    items=revision_mappings,
                )
            )
            responses.raise_for_status()
        return None

    def delete_dm(self, mappings: Sequence[AssetMappingDMRequest], object_3d_space: str, cad_node_space: str) -> None:
        """Delete 3D asset mappings in Data Modeling format.

        Args:
            mappings (Sequence[AssetMappingDMRequest]):
                The 3D asset mapping(s) to delete.
            object_3d_space (str):
                The instance space where the Cognite3DObject are located.
            cad_node_space (str):
                The instance space where the CogniteCADNode are located.
        """
        for endpoint, *_, revision_mappings in self._chunk_mappings_by_endpoint(
            mappings, self.DELETE_DM_MAX_MAPPINGS_PER_REQUEST
        ):
            responses = self._http_client.request_items_retries(
                ItemsRequest2(
                    endpoint_url=self._make_url(f"{endpoint}/delete"),
                    method="DELETE",
                    items=revision_mappings,
                    extra_body_fields={
                        "dmsContextualizationConfig": {
                            "object3DSpace": object_3d_space,
                            "cadNodeSpace": cad_node_space,
                        }
                    },
                )
            )
            responses.raise_for_status()
        return None

    def paginate(
        self,
        model_id: int,
        revision_id: int,
        asset_ids: list[int] | None = None,
        asset_instance_ids: list[str] | None = None,
        node_ids: list[int] | None = None,
        tree_indexes: list[int] | None = None,
        get_dms_instances: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AssetMappingResponse]:
        if sum(param is not None for param in [asset_ids, asset_instance_ids, node_ids, tree_indexes]) > 1:
            raise ValueError("Only one of asset_ids, asset_instance_ids, node_ids, or tree_indexes can be provided.")
        body: dict[str, Any] = {
            "getDmsInstances": get_dms_instances,
        }
        if asset_ids is not None:
            if not (0 < len(asset_ids) <= 100):
                raise ValueError("asset_ids must contain between 1 and 100 IDs.")
            body["filter"] = {"assetIds": asset_ids}
        elif asset_instance_ids is not None:
            if not (0 < len(asset_instance_ids) <= 100):
                raise ValueError("asset_instance_ids must contain between 1 and 100 IDs.")
            body["filter"] = {"assetInstanceIds": asset_instance_ids}
        elif node_ids is not None:
            if not (0 < len(node_ids) <= 100):
                raise ValueError("node_ids must contain between 1 and 100 IDs.")
            body["filter"] = {"nodeIds": node_ids}
        elif tree_indexes is not None:
            if not (0 < len(tree_indexes) <= 100):
                raise ValueError("tree_indexes must contain between 1 and 100 indexes.")
            body["filter"] = {"treeIndexes": tree_indexes}

        endpoint = self.ENDPOINT.format(modelId=model_id, revisionId=revision_id)
        page = self._paginate(limit=limit, cursor=cursor, body=body, endpoint_path=f"{endpoint}/list")
        # Add modelId and revisionId to items since the API does not return them
        for item in page.items:
            object.__setattr__(item, "model_id", model_id)
            object.__setattr__(item, "revision_id", revision_id)
        return page

    def list(
        self,
        model_id: int,
        revision_id: int,
        asset_ids: list[int] | None = None,
        asset_instance_ids: list[str] | None = None,
        node_ids: list[int] | None = None,
        tree_indexes: list[int] | None = None,
        get_dms_instances: bool = False,
        limit: int | None = 100,
    ) -> list[AssetMappingResponse]:
        results: list[AssetMappingResponse] = []
        cursor: str | None = None
        endpoint = self._method_endpoint_map["list"]
        while True:
            request_limit = endpoint.item_limit if limit is None else min(limit - len(results), endpoint.item_limit)
            if request_limit <= 0:
                break
            page = self.paginate(
                model_id=model_id,
                revision_id=revision_id,
                asset_ids=asset_ids,
                asset_instance_ids=asset_instance_ids,
                node_ids=node_ids,
                tree_indexes=tree_indexes,
                get_dms_instances=get_dms_instances,
                limit=request_limit,
                cursor=cursor,
            )
            results.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return results


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.models = ThreeDClassicModelsAPI(http_client)
        self.asset_mappings = ThreeDAssetMappingAPI(http_client)
