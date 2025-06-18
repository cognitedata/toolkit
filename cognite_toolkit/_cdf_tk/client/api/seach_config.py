from __future__ import annotations

from collections.abc import Iterator
from typing import overload

from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig

from cognite_toolkit._cdf_tk.client.data_classes.search_config import SearchConfig, SearchConfigList, SearchConfigWrite


class SearchConfigurationsAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_version = None
        self._RESOURCE_PATH = (
            f"/apps/v1/projects/{self._cognite_client.config.project}/storage/config/apps/search/views"
        )

    @overload
    def __call__(self) -> Iterator[SearchConfig]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[SearchConfigList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[SearchConfig] | Iterator[SearchConfigList]:
        """Iterate over configurations.
        Args:
            chunk_size: The number of configurations to return in each chunk. None will return all configurations.

        Yields:
            SearchConfig or SearchConfigList

        """
        return iter(self.list())

    def __iter__(self) -> Iterator[SearchConfig]:
        return self.__call__()

    def update(self, configuration_update: SearchConfigWrite) -> SearchConfig:
        """Update/Create a Configuration.

        Args:
            configuration_update: The content of the configuration to update

        Returns:
            SearchConfig

        """

        res = self._post(
            url_path=self._RESOURCE_PATH + "/upsert",
            json=configuration_update.dump(),
        )
        return SearchConfig._load(res.json(), cognite_client=self._cognite_client)

    def list(self) -> SearchConfigList:
        res = self._post(url_path=self._RESOURCE_PATH + "/list")
        return SearchConfigList._load(res.json()["items"], cognite_client=self._cognite_client)
