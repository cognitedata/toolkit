from collections.abc import Iterable, Sequence

from pydantic import BaseModel, Field

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.external_data_source import (
    ExternalDataSourceRequest,
    ExternalDataSourceResponse,
)


class ExternalDataSourceUsability(BaseModel):
    external_id: str = Field(alias="externalId")
    usable_version: str | None = Field(default=None, alias="usableVersion")

    model_config = {"populate_by_name": True}


class TransformationExternalDataSourcesAPI(CDFResourceAPI[ExternalDataSourceResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/transformations/external_data", item_limit=1000),
                "delete": Endpoint(method="POST", path="/transformations/external_data/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/transformations/external_data", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExternalDataSourceResponse]:
        return PagedResponse[ExternalDataSourceResponse].model_validate_json(response.body)

    def upsert(self, items: Sequence[ExternalDataSourceRequest]) -> list[ExternalDataSourceResponse]:
        return self._request_item_response(items, "upsert")

    def create(self, items: Sequence[ExternalDataSourceRequest]) -> list[ExternalDataSourceResponse]:
        return self.upsert(items)

    def update(self, items: Sequence[ExternalDataSourceRequest]) -> list[ExternalDataSourceResponse]:
        return self.upsert(items)

    def delete(self, items: Sequence[ExternalId]) -> None:
        self._request_no_response(items, "delete")

    def list(self, limit: int | None = 100) -> list[ExternalDataSourceResponse]:
        return self._list(limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[ExternalDataSourceResponse]]:
        return self._iterate(limit=limit)

    def verify_usability(self, external_id: str) -> ExternalDataSourceUsability:
        request = RequestMessage(
            endpoint_url=self._make_url("/transformations/external_data/usability"),
            method="POST",
            body_content={"externalId": external_id},
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return ExternalDataSourceUsability.model_validate_json(response.body)
