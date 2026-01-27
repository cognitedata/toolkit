from collections.abc import Iterable, Sequence
from typing import TypeVar

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import ThreeDAssetMappingFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequest,
    AssetMappingClassicResponse,
    AssetMappingDMRequest,
    AssetMappingDMResponse,
    ThreeDModelClassicRequest,
    ThreeDModelResponse,
)


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
        self, response: SuccessResponse | ItemsSuccessResponse
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

    def retrieve(self, ids: Sequence[InternalId]) -> list[ThreeDModelResponse]:
        """Retrieve 3D models by their IDs.

        Args:
            ids (Sequence[int]): The IDs of the 3D models to retrieve.

        Returns:
            list[ThreeDModelResponse]: The retrieved 3D model(s).
        """
        return self._request_item_response(ids, "retrieve")

    def update(self, items: Sequence[ThreeDModelClassicRequest]) -> list[ThreeDModelResponse]:
        """Update 3D models in classic format.

        Args:
            items (Sequence[ThreeDModelClassicRequest]): The 3D model(s) to update.

        Returns:
            list[ThreeDModelResponse]: The updated 3D model(s).
        """
        return self._request_item_response(items, "update")

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


class ThreeDClassicAssetMappingAPI(
    CDFResourceAPI[AssetMappingClassicRequest, AssetMappingClassicRequest, AssetMappingClassicResponse]
):
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

    def create(self, mappings: Sequence[AssetMappingClassicRequest]) -> list[AssetMappingClassicResponse]:
        """Create 3D asset mappings.

        Args:
            mappings (Sequence[AssetMappingClassicRequest]):
                The 3D asset mapping(s) to create.

        Returns:
            list[AssetMappingClassicResponse]: The created 3D asset mapping(s).
        """
        results: list[AssetMappingClassicResponse] = []
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

    def delete(self, mappings: Sequence[AssetMappingClassicRequest]) -> None:
        """Delete 3D asset mappings.

        Args:
            mappings (Sequence[AssetMappingClassicRequest]):
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
    ) -> Iterable[list[AssetMappingClassicResponse]]:
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
    ) -> list[AssetMappingClassicResponse]:
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


class ThreeDDMAssetMappingAPI(CDFResourceAPI[AssetMappingDMRequest, AssetMappingDMRequest, AssetMappingDMResponse]):
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
        self, mappings: Sequence[AssetMappingDMRequest], object_3d_space: str, cad_node_space: str
    ) -> list[AssetMappingDMResponse]:
        """Create 3D asset mappings in Data Modeling format.

        Args:
            mappings (Sequence[AssetMappingDMRequest]):
                The 3D asset mapping(s) to create
            object_3d_space (str):
                The instance space where the Cognite3DObject are located.
            cad_node_space (str):
                The instance space where the CogniteCADNode are located.
        Returns:
            list[AssetMappingDMResponse]: The created 3D asset mapping(s).
        """
        results: list[AssetMappingDMResponse] = []
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

    def delete(self, mappings: Sequence[AssetMappingDMRequest], object_3d_space: str, cad_node_space: str) -> None:
        """Delete 3D asset mappings in Data Modeling format.

        Args:
            mappings (Sequence[AssetMappingDMRequest]):
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
    ) -> Iterable[list[AssetMappingDMResponse]]:
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
    ) -> list[AssetMappingDMResponse]:
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


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.models_classic = ThreeDClassicModelsAPI(http_client)
        self.asset_mappings_classic = ThreeDClassicAssetMappingAPI(http_client)
        self.asset_mappings_dm = ThreeDDMAssetMappingAPI(http_client)
