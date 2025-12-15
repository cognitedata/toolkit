from collections.abc import Sequence

from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.cdf_client._client import CDFClient
from cognite_toolkit._cdf_tk.client.data_classes.assets import AssetRequest, AssetResponse


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
        raise NotImplementedError()

    def retrieve(self, ids: Sequence[int], external_ids: SequenceNotStr[str]) -> list[AssetResponse]:
        """Retrieve assets by their IDs or external IDs.

        Args:
            ids (Sequence[int]): The IDs of the assets to retrieve.
            external_ids (Sequence[str]): The external IDs of the assets to retrieve.
        Returns:
            list[AssetResponse]: The retrieved assets.
        """
        raise NotImplementedError()

    def update(self, assets: Sequence[AssetRequest]) -> list[AssetResponse]:
        """Update assets.

        Args:
            assets (Sequence[AssetRequest]): The assets to update.
        Returns:
            list[AssetResponse]: The updated assets.
        """
        raise NotImplementedError()

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
        raise NotImplementedError()
