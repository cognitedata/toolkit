from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.infield import InfieldLocationConfig
from cognite_toolkit._cdf_tk.client.data_classes.instance_api import (
    InstanceSource,
    NodeIdentifier,
    NodeRequestItem,
    NodeResult,
    ViewReference,
)
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemsRequest


class InfieldConfigAPI:
    ENDPOINT = "/models/instances"
    view_id = ViewReference(space="cdf_infield", external_id="InFieldLocationConfig", version="v1")

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self._config = http_client.config

    def apply(self, items: Sequence[InfieldLocationConfig]) -> list[NodeResult]:
        if len(items) > 1000:
            raise ValueError("Cannot apply more than 1000 InfieldLocationConfig items at once.")
        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=[
                    NodeRequestItem(
                        space=item.space,
                        external_id=item.external_id,
                        sources=[InstanceSource(source=self.view_id, resource=item)],
                    )
                    for item in items
                ],
            )
        )
        responses.raise_for_status()
        return PagedResponse[NodeResult].model_validate(responses.get_first_body()).items

    def retrieve(self, items: Sequence[NodeIdentifier]) -> list[InfieldLocationConfig]:
        if len(items) > 1000:
            raise ValueError("Cannot retrieve more than 1000 InfieldLocationConfig items at once.")
        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/byids"),
                method="POST",
                items=[NodeIdentifier(space=item.space, external_id=item.external_id) for item in items],
            )
        )
        responses.raise_for_status()
        return PagedResponse[InfieldLocationConfig].model_validate(responses.get_first_body()).items

    def delete(self, items: Sequence[NodeIdentifier]) -> list[NodeIdentifier]:
        if len(items) > 1000:
            raise ValueError("Cannot delete more than 1000 InfieldLocationConfig items at once.")
        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/delete"),
                method="POST",
                items=[NodeIdentifier(space=item.space, external_id=item.external_id) for item in items],
            )
        )
        responses.raise_for_status()
        return PagedResponse[NodeIdentifier].model_validate(responses.get_first_body()).items


class InfieldAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self.config = InfieldConfigAPI(http_client)
