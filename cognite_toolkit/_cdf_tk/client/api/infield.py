from collections.abc import Sequence

from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse, QueryResponse
from cognite_toolkit._cdf_tk.client.data_classes.infield import DataExplorationConfig, InfieldLocationConfig
from cognite_toolkit._cdf_tk.client.data_classes.instance_api import (
    InstanceResponseItem,
    InstanceResult,
    NodeIdentifier,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemsRequest, SimpleBodyRequest


class InfieldConfigAPI:
    ENDPOINT = "/models/instances"

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
        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=[item for sublist in request_items for item in sublist],
            )
        )
        responses.raise_for_status()
        return PagedResponse[InstanceResult].model_validate(responses.get_first_body()).items

    def retrieve(self, items: Sequence[NodeIdentifier]) -> list[InfieldLocationConfig]:
        if len(items) > 100:
            raise ValueError("Cannot retrieve more than 100 InfieldLocationConfig items at once.")
        responses = self._http_client.request_with_retries(
            SimpleBodyRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/query"),
                method="POST",
                body_content={
                    "with": {
                        "locationConfig": {
                            "limit": len(items),
                            "nodes": {
                                "filter": {
                                    "instanceReferences": [
                                        {"space": item.space, "externalId": item.external_id} for item in items
                                    ]
                                },
                            },
                        },
                        "dataExplorationConfig": {
                            "nodes": {
                                "from": "locationConfig",
                                "through": {
                                    "source": InfieldLocationConfig.VIEW_ID.dump(),
                                    "identifier": "dataExplorationConfig",
                                },
                            }
                        },
                    },
                    "select": {
                        "locationConfig": {
                            "sources": [{"source": InfieldLocationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                        },
                        "dataExplorationConfig": {
                            "sources": [{"source": DataExplorationConfig.VIEW_ID.dump(), "properties": ["*"]}],
                        },
                    },
                },
            )
        )
        responses.raise_for_status()
        parsed_response = QueryResponse[InstanceResponseItem].model_validate(responses.get_first_body())
        data_exploration_results = (
            DataExplorationConfig.model_validate(
                item.get_properties_for_source(DataExplorationConfig.VIEW_ID, include_identifier=True)
            )
            for item in parsed_response.items["dataExplorationConfig"]
        )
        data_exploration_config_map = {(dec.space, dec.external_id): dec for dec in data_exploration_results}
        result: list[InfieldLocationConfig] = []
        for item in parsed_response.items["locationConfig"]:
            properties = item.get_properties_for_source(InfieldLocationConfig.VIEW_ID, include_identifier=True)
            data_exploration = properties.pop("dataExplorationConfig", None)
            if isinstance(data_exploration, dict):
                space = data_exploration["space"]
                external_id = data_exploration["externalId"]
                if (space, external_id) not in data_exploration_config_map:
                    HighSeverityWarning(
                        f"DataExplorationConfig with space '{space}' and externalId '{external_id}' referenced in InfieldLocationConfig '{properties['externalId']}' was not found in the retrieved results."
                    ).print_warning(console=self._console)
                else:
                    # Pydantic allow already validated models to be assigned to fields
                    properties["dataExplorationConfig"] = data_exploration_config_map[(space, external_id)]  # type: ignore[assignment,index]
            else:
                HighSeverityWarning(
                    f"InfieldLocationConfig '{properties['externalId']}' is missing a valid DataExplorationConfig reference."
                ).print_warning(console=self._console)
            result.append(InfieldLocationConfig.model_validate(properties))
        return result

    def delete(self, items: Sequence[NodeIdentifier]) -> list[NodeIdentifier]:
        if len(items) > 500:
            raise ValueError("Cannot delete more than 500 InfieldLocationConfig items at once.")
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
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self.config = InfieldConfigAPI(http_client, console)
