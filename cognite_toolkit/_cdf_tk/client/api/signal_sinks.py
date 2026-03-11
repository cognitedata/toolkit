from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId
from cognite_toolkit._cdf_tk.client.resource_classes.signal_sink import SignalSinkRequest, SignalSinkResponse


class SignalSinksAPI(CDFResourceAPI[SignalSinkResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/processes/signals/sinks", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/processes/signals/sinks/byids", item_limit=100),
                "update": Endpoint(method="POST", path="/processes/signals/sinks/update", item_limit=100),
                "delete": Endpoint(method="POST", path="/processes/signals/sinks/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/processes/signals/sinks", item_limit=100),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SignalSinkResponse]:
        return PagedResponse[SignalSinkResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SignalSinkRequest]) -> list[SignalSinkResponse]:
        return self._request_item_response(items, "create")

    def retrieve(self, ids: Sequence[SignalSinkId], ignore_unknown_ids: bool = False) -> list[SignalSinkResponse]:
        return self._request_item_response(ids, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def update(
        self, items: Sequence[SignalSinkRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[SignalSinkResponse]:
        return self._update(items, mode=mode)

    def delete(self, ids: Sequence[SignalSinkId], ignore_unknown_ids: bool = False) -> None:
        self._request_no_response(ids, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(self, limit: int | None = 100) -> Iterable[list[SignalSinkResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[SignalSinkResponse]:
        return self._list(limit=limit)
