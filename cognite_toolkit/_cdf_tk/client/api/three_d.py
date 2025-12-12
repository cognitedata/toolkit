from collections.abc import Sequence

from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelClassicRequest, ThreeDModelResponse
from cognite_toolkit._cdf_tk.utils.http_client import (
    HTTPClient,
    ItemsRequest,
    RequestMessage2,
    SimpleBodyRequest,
)
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
        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=list(models),
            )
        )
        responses.raise_for_status()
        body = responses.get_first_body()
        return PagedResponse[ThreeDModelResponse].model_validate(body).items

    def delete(self, ids: Sequence[int]) -> None:
        """Delete 3D models by their IDs.

        Args:
            ids (Sequence[int]): The IDs of the 3D models to delete.
        """
        if not ids:
            return None
        if len(ids) > self.MAX_MODELS_PER_DELETE_REQUEST:
            raise ValueError("Cannot delete more than 1000 3D models in a single request.")
        responses = self._http_client.request_with_retries(
            SimpleBodyRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT + "/delete"),
                method="POST",
                body_content={"items": [{"id": id_} for id_ in ids]},
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


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self.models = ThreeDModelAPI(http_client, console)
