from collections.abc import Sequence
from dataclasses import dataclass

from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.cdf_client._client import CDFClient
from cognite_toolkit._cdf_tk.client.data_classes.assets import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.utils.http_client import (
    ItemsFailedRequest2,
    ItemsFailedResponse2,
)


@dataclass
class HTTPResult:
    successes: list[AssetResponse]
    failed_requests: list[ItemsFailedRequest2]
    failed_responses: list[ItemsFailedResponse2]


class AssetsAPI:
    ENDPOINT = "/assets"
    CREATE_REQUEST_LIMIT = 1000
    RETRIEVE_REQUEST_LIMIT = 1000
    UPDATE_REQUEST_LIMIT = 1000
    DELETE_REQUEST_LIMIT = 1000

    def __init__(self, cdf_client: CDFClient) -> None:
        self._cdf_client = cdf_client

    def create(self, assets: Sequence[AssetRequest]) -> list[AssetResponse]:
        """Create assets.

        Args:
            assets (Sequence[AssetRequest]): The assets to create.
        Returns:
            list[AssetResponse]: The created assets.
        """
        return self._cdf_client.request_resource_items(
            items=assets,
            endpoint_url=self._cdf_client.config.create_api_url(self.ENDPOINT),
            method="POST",
            request_limit=self.CREATE_REQUEST_LIMIT,
            response_type=AssetResponse,
        )

    def retrieve(
        self,
        ids: Sequence[int],
        external_ids: SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
        aggregated_properties: bool = False,
    ) -> list[AssetResponse]:
        """Retrieve assets by their IDs or external IDs.

        Args:
            ids (Sequence[int]): The IDs of the assets to retrieve.
            external_ids (Sequence[str]): The external IDs of the assets to retrieve.
        Returns:
            list[AssetResponse]: The retrieved assets.
        """
        return self._cdf_client.request_resource_items(
            items=...,
            endpoint_url=self._cdf_client.config.create_api_url(f"{self.ENDPOINT}/byids"),
            method="POST",
            request_limit=self.RETRIEVE_REQUEST_LIMIT,
            response_type=AssetResponse,
            extra_body_fields={
                "ignoreUnknownIds": ignore_unknown_ids,
                "aggregatedProperties": ["childCount", "depth", "path"] if aggregated_properties else [],
            },
        )

    def update(self, assets: Sequence[AssetRequest]) -> list[AssetResponse]:
        """Update assets.

        Args:
            assets (Sequence[AssetRequest]): The assets to update.
        Returns:
            list[AssetResponse]: The updated assets.
        """
        self._cdf_client.request_resource_items(
            items=assets,
            endpoint_url=self._cdf_client.config.create_api_url(f"{self.ENDPOINT}/update"),
            method="POST",
            request_limit=self.RETRIEVE_REQUEST_LIMIT,
            response_type=AssetResponse,
        )

    def delete(
        self,
        ids: Sequence[int],
        external_ids: SequenceNotStr[str],
        recursive: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """Delete assets by their IDs or external IDs.

        Args:
            ids (Sequence[int]): The IDs of the assets to delete.
            external_ids (Sequence[str]): The external IDs of the assets to delete.
        """
        self._cdf_client.request_resource_items(
            items=...,
            endpoint_url=self._cdf_client.config.create_api_url(f"{self.ENDPOINT}/delete"),
            method="POST",
            request_limit=self.RETRIEVE_REQUEST_LIMIT,
            response_type=AssetResponse,
            extra_body_fields={
                "recursive": recursive,
                "ignoreUnknownIds": ignore_unknown_ids,
            },
        )
