from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any, TypeVar

from pydantic import TypeAdapter
from rich.console import Console

from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.data_classes.three_d import (
    AssetMappingClassicRequest,
    AssetMappingDMRequest,
    AssetMappingResponse,
    ThreeDModelClassicRequest,
    ThreeDModelResponse,
)
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsRequest2,
    RequestMessage2,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType


class ThreeDModelAPI:
    ENDPOINT = "/3d/models"
    MAX_CLASSIC_MODELS_PER_CREATE_REQUEST = 1000
    MAX_MODELS_PER_DELETE_REQUEST = 1000
    _LIST_REQUEST_MAX_LIMIT = 1000

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def create(self, models: Sequence[ThreeDModelClassicRequest]) -> list[ThreeDModelResponse]:
        """Create 3D models in classic format.

        Args:
            models (Sequence[ThreeDModelClassicRequest]): The 3D model(s) to create.

        Returns:
            list[ThreeDModelResponse]: The created 3D model(s).
        """
        if not models:
            return []
        if len(models) > self.MAX_CLASSIC_MODELS_PER_CREATE_REQUEST:
            raise ValueError("Cannot create more than 1000 3D models in a single request.")
        responses = self._http_client.request_items_retries(
            ItemsRequest2(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=models,
            )
        )
        responses.raise_for_status()
        return TypeAdapter(list[ThreeDModelResponse]).validate_python(responses.get_items())

    def delete(self, ids: Sequence[int]) -> None:
        """Delete 3D models by their IDs.

        Args:
            ids (Sequence[int]): The IDs of the 3D models to delete.
        """
        if not ids:
            return None
        if len(ids) > self.MAX_MODELS_PER_DELETE_REQUEST:
            raise ValueError("Cannot delete more than 1000 3D models in a single request.")
        responses = self._http_client.request_items_retries(
            ItemsRequest2(
                endpoint_url=self._config.create_api_url(self.ENDPOINT + "/delete"),
                method="POST",
                items=InternalId.from_ids(list(ids)),
            )
        )
        responses.raise_for_status()

    def iterate(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDModelResponse]:
        if not (0 < limit <= self._LIST_REQUEST_MAX_LIMIT):
            raise ValueError(f"Limit must be between 1 and {self._LIST_REQUEST_MAX_LIMIT}, got {limit}.")
        parameters: dict[str, PrimitiveType] = {
            # There is a bug in the API. The parameter includeRevisionInfo is expected to be lower case and not
            # camel case as documented. You get error message: Unrecognized query parameter includeRevisionInfo,
            # did you mean includerevisioninfo?
            "includerevisioninfo": include_revision_info,
            "limit": limit,
        }
        if published is not None:
            parameters["published"] = published
        if cursor is not None:
            parameters["cursor"] = cursor
        responses = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="GET",
                parameters=parameters,
            )
        )
        success_response = responses.get_success_or_raise()
        return PagedResponse[ThreeDModelResponse].model_validate(success_response.body_json)

    def list(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int | None = 100,
        cursor: str | None = None,
    ) -> list[ThreeDModelResponse]:
        results: list[ThreeDModelResponse] = []
        while True:
            request_limit = (
                self._LIST_REQUEST_MAX_LIMIT
                if limit is None
                else min(limit - len(results), self._LIST_REQUEST_MAX_LIMIT)
            )
            if request_limit <= 0:
                break
            page = self.iterate(
                published=published,
                include_revision_info=include_revision_info,
                limit=request_limit,
                cursor=cursor,
            )
            results.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return results


T_RequestMapping = TypeVar("T_RequestMapping", bound=AssetMappingClassicRequest | AssetMappingDMRequest)


class ThreeDAssetMappingAPI:
    ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/mappings"
    CREATE_CLASSIC_MAX_MAPPINGS_PER_REQUEST = 1000
    CREATE_DM_MAX_MAPPINGS_PER_REQUEST = 100
    DELETE_CLASSIC_MAX_MAPPINGS_PER_REQUEST = 1000
    DELETE_DM_MAX_MAPPINGS_PER_REQUEST = 100
    LIST_REQUEST_MAX_LIMIT = 1000

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

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
                    endpoint_url=self._config.create_api_url(endpoint),
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
                    endpoint_url=self._config.create_api_url(endpoint),
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
                    endpoint_url=self._config.create_api_url(f"{endpoint}/delete"),
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
                    endpoint_url=self._config.create_api_url(f"{endpoint}/delete"),
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

    def iterate(
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
        if not (0 < limit <= self.LIST_REQUEST_MAX_LIMIT):
            raise ValueError(f"Limit must be between 1 and {self.LIST_REQUEST_MAX_LIMIT}, got {limit}.")
        if sum(param is not None for param in [asset_ids, asset_instance_ids, node_ids, tree_indexes]) > 1:
            raise ValueError("Only one of asset_ids, asset_instance_ids, node_ids, or tree_indexes can be provided.")
        body: dict[str, Any] = {
            "getDmsInstances": get_dms_instances,
            "limit": limit,
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
        if cursor is not None:
            body["cursor"] = cursor

        endpoint = self.ENDPOINT.format(modelId=model_id, revisionId=revision_id)
        responses = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._config.create_api_url(f"{endpoint}/list"),
                method="POST",
                body_content=body,
            )
        )
        success_response = responses.get_success_or_raise()
        body_json = success_response.body_json
        # Add modelId and revisionId to items since the API does not return them
        for item in body_json.get("items", []):
            item["modelId"] = model_id
            item["revisionId"] = revision_id
        return PagedResponse[AssetMappingResponse].model_validate(body_json)

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
        while True:
            request_limit = (
                self.LIST_REQUEST_MAX_LIMIT if limit is None else min(limit - len(results), self.LIST_REQUEST_MAX_LIMIT)
            )
            if request_limit <= 0:
                break
            page = self.iterate(
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
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self.models = ThreeDModelAPI(http_client, console)
        self.asset_mappings = ThreeDAssetMappingAPI(http_client, console)
