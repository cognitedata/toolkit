"""Apps API: Dune apps as classic files under /dune-apps/ (same pattern as Streamlit + /files)."""

from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import DuneAppFilter
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse


class AppsAPI(CDFResourceAPI[AppResponse]):
    """Dune apps are file metadata objects under ``/dune-apps/`` with a zip uploaded to ``uploadUrl``."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/files", item_limit=1, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/files/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/files/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/files/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/files/list", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[AppResponse]:
        return PagedResponse[AppResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[AppRequest], overwrite: bool = False) -> list[AppResponse]:
        endpoint = self._method_endpoint_map["create"]
        results: list[AppResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=item.dump(),
                parameters={"overwrite": overwrite},
            )
            response = self._http_client.request_single_retries(request)
            result = response.get_success_or_raise(request)
            results.append(AppResponse.model_validate_json(result.body))
        return results

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[AppResponse]:
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(self, items: Sequence[AppRequest], mode: Literal["patch", "replace"] = "replace") -> list[AppResponse]:
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: DuneAppFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AppResponse]:
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": (filter or DuneAppFilter()).dump()},
        )

    def iterate(
        self,
        filter: DuneAppFilter | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[AppResponse]]:
        return self._iterate(
            limit=limit,
            body={"filter": (filter or DuneAppFilter()).dump()},
        )

    def list(
        self,
        filter: DuneAppFilter | None = None,
        limit: int | None = 100,
    ) -> list[AppResponse]:
        return self._list(
            limit=limit,
            body={"filter": (filter or DuneAppFilter()).dump()},
        )
