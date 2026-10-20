import builtins
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
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequestUnion,
    ExternalDataSourceResponseUnion,
    ExternalDataSourceUsabilityResponse,
    RotatedKeysResponse,
    RotateKeysRequest,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class TransformationExternalDataSourcesAPI(CDFResourceAPI[ExternalDataSourceResponseUnion]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/transformations/externaldata", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/transformations/externaldata/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/transformations/externaldata/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/transformations/externaldata", item_limit=1000),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExternalDataSourceResponseUnion]:
        return PagedResponse[ExternalDataSourceResponseUnion].model_validate_json(response.body)

    def create(self, items: Sequence[ExternalDataSourceRequestUnion]) -> builtins.list[ExternalDataSourceResponseUnion]:
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[ExternalDataSourceResponseUnion]:
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        if ignore_unknown_ids:
            self._request_item_split_retries_no_response(items, "delete")
        else:
            self._request_no_response(items, "delete")

    def list(
        self, limit: int | None = 100, format: Literal["one_lake", "snowflake"] | None = None
    ) -> builtins.list[ExternalDataSourceResponseUnion]:
        return self._list(limit=limit, params={"format": format})

    def iterate(
        self, limit: int | None = 100, format: Literal["one_lake", "snowflake"] | None = None
    ) -> Iterable[Sequence[ExternalDataSourceResponseUnion]]:
        return self._iterate(limit=limit, params={"format": format})

    def rotate_keys(self, items: Sequence[RotateKeysRequest]) -> builtins.list[RotatedKeysResponse]:
        rotated: list[RotatedKeysResponse] = []
        for chunk in chunker_sequence(items, 10):
            request = RequestMessage(
                endpoint_url=self._make_url("/transformations/externaldata/rotatekeys"),
                method="POST",
                body_content={"items": [item.dump() for item in chunk]},
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            rotated.extend(ResponseItems[RotatedKeysResponse].model_validate_json(response.body).items)
        return rotated

    def verify_usability(self, external_id: str) -> ExternalDataSourceUsabilityResponse:
        request = RequestMessage(
            endpoint_url=self._make_url("/transformations/externaldata/usability"),
            method="POST",
            body_content={"externalId": external_id},
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return ExternalDataSourceUsabilityResponse.model_validate_json(response.body)
