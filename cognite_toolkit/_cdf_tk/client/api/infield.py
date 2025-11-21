from collections.abc import Sequence
from typing import Any

from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse, QueryResponse
from cognite_toolkit._cdf_tk.client.data_classes.infield import (
    InFieldCDMLocationConfig,
    InfieldLocationConfig,
)
from cognite_toolkit._cdf_tk.client.data_classes.instance_api import (
    InstanceResponseItem,
    InstanceResult,
    NodeIdentifier,
)
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemsRequest, SimpleBodyRequest


class InfieldConfigAPI:
    ENDPOINT = "/models/instances"
    LOCATION_REF = "locationConfig"

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def apply(self, items: Sequence[InfieldLocationConfig]) -> list[InstanceResult]:
        if len(items) > 500:
            raise ValueError("Cannot apply more than 500 InfieldLocationConfig items at once.")

        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=[item.as_request_item() for item in items],
            )
        )
        responses.raise_for_status()
        return PagedResponse[InstanceResult].model_validate(responses.get_first_body()).items

    def retrieve(self, items: Sequence[NodeIdentifier]) -> list[InfieldLocationConfig]:
        if len(items) > 100:
            raise ValueError("Cannot retrieve more than 100 InfieldLocationConfig items at once.")
        if not items:
            return []
        responses = self._http_client.request_with_retries(
            SimpleBodyRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/query"),
                method="POST",
                body_content=self._retrieve_query(items),
            )
        )
        responses.raise_for_status()
        parsed_response = QueryResponse[InstanceResponseItem].model_validate(responses.get_first_body())
        return self._parse_retrieve_response(parsed_response)

    def delete(self, items: Sequence[InfieldLocationConfig]) -> list[NodeIdentifier]:
        if len(items) > 500:
            raise ValueError("Cannot delete more than 500 InfieldLocationConfig items at once.")

        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/delete"),
                method="POST",
                items=[item.as_id() for item in items],
            )
        )
        responses.raise_for_status()
        return PagedResponse[NodeIdentifier].model_validate(responses.get_first_body()).items

    @classmethod
    def _retrieve_query(cls, items: Sequence[NodeIdentifier]) -> dict[str, Any]:
        return {
            "with": {
                cls.LOCATION_REF: {
                    "limit": len(items),
                    "nodes": {
                        "filter": {
                            "instanceReferences": [
                                {"space": item.space, "externalId": item.external_id} for item in items
                            ]
                        },
                    },
                },
            },
            "select": {
                cls.LOCATION_REF: {
                    "sources": [{"source": InfieldLocationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                },
            },
        }

    def _parse_retrieve_response(
        self, parsed_response: QueryResponse[InstanceResponseItem]
    ) -> list[InfieldLocationConfig]:
        return [
            InfieldLocationConfig.model_validate(
                item.get_properties_for_source(InfieldLocationConfig.VIEW_ID, include_identifier=True)
            )
            for item in parsed_response.items[self.LOCATION_REF]
        ]


class InFieldCDMLocationConfigAPI:
    ENDPOINT = "/models/instances"
    LOCATION_REF = "locationConfig"

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def apply(self, items: Sequence[InFieldCDMLocationConfig]) -> list[InstanceResult]:
        if len(items) > 500:
            raise ValueError("Cannot apply more than 500 InFieldCDMLocationConfig items at once.")

        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=[item.as_request_item() for item in items],
            )
        )
        responses.raise_for_status()
        return PagedResponse[InstanceResult].model_validate(responses.get_first_body()).items

    def retrieve(self, items: Sequence[NodeIdentifier]) -> list[InFieldCDMLocationConfig]:
        if len(items) > 100:
            raise ValueError("Cannot retrieve more than 100 InFieldCDMLocationConfig items at once.")
        if not items:
            return []
        responses = self._http_client.request_with_retries(
            SimpleBodyRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/query"),
                method="POST",
                body_content=self._retrieve_query(items),
            )
        )
        responses.raise_for_status()
        parsed_response = QueryResponse[InstanceResponseItem].model_validate(responses.get_first_body())
        return self._parse_retrieve_response(parsed_response)

    def delete(self, items: Sequence[InFieldCDMLocationConfig]) -> list[NodeIdentifier]:
        if len(items) > 500:
            raise ValueError("Cannot delete more than 500 InFieldCDMLocationConfig items at once.")

        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/delete"),
                method="POST",
                items=[item.as_id() for item in items],
            )
        )
        responses.raise_for_status()
        return PagedResponse[NodeIdentifier].model_validate(responses.get_first_body()).items

    @classmethod
    def _retrieve_query(cls, items: Sequence[NodeIdentifier]) -> dict[str, Any]:
        return {
            "with": {
                cls.LOCATION_REF: {
                    "limit": len(items),
                    "nodes": {
                        "filter": {
                            "instanceReferences": [
                                {"space": item.space, "externalId": item.external_id} for item in items
                            ]
                        },
                    },
                },
            },
            "select": {
                cls.LOCATION_REF: {
                    "sources": [{"source": InFieldCDMLocationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                },
            },
        }

    def _parse_retrieve_response(
        self, parsed_response: QueryResponse[InstanceResponseItem]
    ) -> list[InFieldCDMLocationConfig]:
        return [
            InFieldCDMLocationConfig.model_validate(
                item.get_properties_for_source(InFieldCDMLocationConfig.VIEW_ID, include_identifier=True)
            )
            for item in parsed_response.items[self.LOCATION_REF]
        ]


class InfieldAPI:
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self.config = InfieldConfigAPI(http_client, console)
        self.cdm_location_config = InFieldCDMLocationConfigAPI(http_client, console)
