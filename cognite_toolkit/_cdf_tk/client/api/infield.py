from collections.abc import Sequence
from typing import Any, cast

from pydantic import TypeAdapter
from rich.console import Console

from cognite_toolkit._cdf_tk.client.cdf_client.responses import QueryResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsRequest,
    RequestMessage,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    DataExplorationConfig,
    InFieldCDMLocationConfig,
    InfieldLocationConfig,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    InstanceResponseItem,
    InstanceResult,
    TypedNodeIdentifier,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning


class InfieldConfigAPI:
    ENDPOINT = "/models/instances"
    LOCATION_REF = "locationConfig"
    EXPLORATION_REF = "explorerConfig"
    # We know that this key exists and it has alias set.
    DATA_EXPLORATION_PROP_ID = cast(str, InfieldLocationConfig.model_fields["data_exploration_config"].alias)

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def apply(self, items: Sequence[InfieldLocationConfig]) -> list[InstanceResult]:
        if len(items) > 500:
            raise ValueError("Cannot apply more than 500 InfieldLocationConfig items at once.")

        request_items = (
            [item.as_request_item()]
            if item.data_exploration_config is None
            else [item.as_request_item(), item.data_exploration_config.as_request_item()]
            for item in items
        )
        responses = self._http_client.request_items_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=[item for sublist in request_items for item in sublist],
            )
        )
        responses.raise_for_status()
        return TypeAdapter(list[InstanceResult]).validate_python(responses.get_items())

    def retrieve(self, items: Sequence[TypedNodeIdentifier]) -> list[InfieldLocationConfig]:
        if len(items) > 100:
            raise ValueError("Cannot retrieve more than 100 InfieldLocationConfig items at once.")
        if not items:
            return []
        response = self._http_client.request_single_retries(
            RequestMessage(
                # We use the query endpoint to be able to retrieve linked DataExplorationConfig items
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/query"),
                method="POST",
                body_content=self._retrieve_query(items),
            )
        )
        success = response.get_success_or_raise()
        parsed_response = QueryResponse[InstanceResponseItem].model_validate(success.body_json)
        return self._parse_retrieve_response(parsed_response)

    def delete(self, items: Sequence[InfieldLocationConfig]) -> list[TypedNodeIdentifier]:
        if len(items) > 500:
            raise ValueError("Cannot delete more than 500 InfieldLocationConfig items at once.")

        identifiers = (
            [item.as_id()]
            if item.data_exploration_config is None
            else [item.as_id(), item.data_exploration_config.as_id()]
            for item in items
        )
        responses = self._http_client.request_items_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/delete"),
                method="POST",
                items=[identifier for sublist in identifiers for identifier in sublist],
            )
        )
        responses.raise_for_status()
        return TypeAdapter(list[TypedNodeIdentifier]).validate_python(responses.get_items())

    @classmethod
    def _retrieve_query(cls, items: Sequence[TypedNodeIdentifier]) -> dict[str, Any]:
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
                cls.EXPLORATION_REF: {
                    "nodes": {
                        "from": "locationConfig",
                        "direction": "outwards",
                        "through": {
                            "source": InfieldLocationConfig.VIEW_ID.dump(),
                            "identifier": cls.DATA_EXPLORATION_PROP_ID,
                        },
                    }
                },
            },
            "select": {
                cls.LOCATION_REF: {
                    "sources": [{"source": InfieldLocationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                },
                cls.EXPLORATION_REF: {
                    "sources": [{"source": DataExplorationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                },
            },
        }

    def _parse_retrieve_response(
        self, parsed_response: QueryResponse[InstanceResponseItem]
    ) -> list[InfieldLocationConfig]:
        data_exploration_results = (
            DataExplorationConfig.model_validate(
                item.get_properties_for_source(DataExplorationConfig.VIEW_ID, include_identifier=True)
            )
            for item in parsed_response.items[self.EXPLORATION_REF]
        )
        data_exploration_config_map = {(dec.space, dec.external_id): dec for dec in data_exploration_results}
        result: list[InfieldLocationConfig] = []
        for item in parsed_response.items[self.LOCATION_REF]:
            properties = item.get_properties_for_source(InfieldLocationConfig.VIEW_ID, include_identifier=True)
            data_exploration = properties.pop(self.DATA_EXPLORATION_PROP_ID, None)
            if isinstance(data_exploration, dict):
                space = data_exploration["space"]
                external_id = data_exploration["externalId"]
                if (space, external_id) not in data_exploration_config_map:
                    HighSeverityWarning(
                        f"{self.DATA_EXPLORATION_PROP_ID} with space '{space}' and externalId '{external_id}' referenced in InfieldLocationConfig '{properties['externalId']}' was not found in the retrieved results."
                    ).print_warning(console=self._console)
                else:
                    # Pydantic allow already validated models to be assigned to fields
                    properties[self.DATA_EXPLORATION_PROP_ID] = data_exploration_config_map[(space, external_id)]  # type: ignore[assignment,index]
            else:
                HighSeverityWarning(
                    f"InfieldLocationConfig '{properties['externalId']}' is missing a valid {self.DATA_EXPLORATION_PROP_ID} reference."
                ).print_warning(console=self._console)
            result.append(InfieldLocationConfig.model_validate(properties))
        return result


class InFieldCDMConfigAPI:
    ENDPOINT = "/models/instances"
    LOCATION_REF = "cdmLocationConfig"

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def apply(self, items: Sequence[InFieldCDMLocationConfig]) -> list[InstanceResult]:
        if len(items) > 500:
            raise ValueError("Cannot apply more than 500 InFieldCDMLocationConfig items at once.")

        request_items = [item.as_request_item() for item in items]
        results = self._http_client.request_items_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=request_items,
            )
        )
        results.raise_for_status()
        return TypeAdapter(list[InstanceResult]).validate_python(results.get_items())

    def retrieve(self, items: Sequence[TypedNodeIdentifier]) -> list[InFieldCDMLocationConfig]:
        if len(items) > 100:
            raise ValueError("Cannot retrieve more than 100 InFieldCDMLocationConfig items at once.")
        if not items:
            return []
        result = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/query"),
                method="POST",
                body_content=self._retrieve_query(items),
            )
        )
        success = result.get_success_or_raise()
        parsed_response = QueryResponse[InstanceResponseItem].model_validate(success.body_json)
        return self._parse_retrieve_response(parsed_response)

    def delete(self, items: Sequence[InFieldCDMLocationConfig]) -> list[TypedNodeIdentifier]:
        if len(items) > 500:
            raise ValueError("Cannot delete more than 500 InFieldCDMLocationConfig items at once.")

        identifiers = [item.as_id() for item in items]
        responses = self._http_client.request_items_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/delete"),
                method="POST",
                items=identifiers,
            )
        )
        responses.raise_for_status()
        return TypeAdapter(list[TypedNodeIdentifier]).validate_python(responses.get_items())

    @classmethod
    def _retrieve_query(cls, items: Sequence[TypedNodeIdentifier]) -> dict[str, Any]:
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
        result: list[InFieldCDMLocationConfig] = []
        for item in parsed_response.items[self.LOCATION_REF]:
            properties = item.get_properties_for_source(InFieldCDMLocationConfig.VIEW_ID, include_identifier=True)
            result.append(InFieldCDMLocationConfig.model_validate(properties))
        return result


class InfieldAPI:
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self.config = InfieldConfigAPI(http_client, console)
        self.cdm_config = InFieldCDMConfigAPI(http_client, console)
