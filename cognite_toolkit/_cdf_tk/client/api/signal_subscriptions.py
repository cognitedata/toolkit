from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    SignalSubscriptionRequest,
    SignalSubscriptionResponse,
)


class SignalSubscriptionsAPI(CDFResourceAPI[SignalSubscriptionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/processes/signals/subscriptions", item_limit=10),
                "update": Endpoint(method="POST", path="/processes/signals/subscriptions/update", item_limit=10),
                "delete": Endpoint(method="POST", path="/processes/signals/subscriptions/delete", item_limit=100),
                "list": Endpoint(method="POST", path="/processes/signals/subscriptions/list", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SignalSubscriptionResponse]:
        return PagedResponse[SignalSubscriptionResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SignalSubscriptionRequest]) -> list[SignalSubscriptionResponse]:
        return self._request_item_response(items, "create")

    def update(
        self, items: Sequence[SignalSubscriptionRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[SignalSubscriptionResponse]:
        return self._update(items, mode=mode)

    def delete(self, ids: Sequence[SignalSubscriptionId], ignore_unknown_ids: bool = False) -> None:
        self._request_no_response(ids, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(self, limit: int | None = 100) -> Iterable[list[SignalSubscriptionResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[SignalSubscriptionResponse]:
        return self._list(limit=limit)
