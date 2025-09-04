from urllib.parse import urljoin

from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig

from cognite_toolkit._cdf_tk.client.data_classes.search_config import SearchConfig, SearchConfigList, SearchConfigWrite


class SearchConfigurationsAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)

    def _get_base_url_with_base_path(self) -> str:
        """
        This method is overridden to provide the correct base path for the Search Configurations API.
        This method in base class APIClient appends /api/{api_version}/ to the base URL,
        but for Search Configurations API, we need a different path structure.
        """
        base_path = ""
        if self._api_version:
            base_path = f"/apps/{self._api_version}/projects/{self._config.project}/storage/config/apps/search/views"
        return urljoin(self._config.base_url, base_path)

    def upsert(self, configuration_update: SearchConfigWrite) -> SearchConfig:
        """Update/Create a Configuration.

        Args:
            configuration_update: The content of the configuration to update

        Returns:
            SearchConfig

        """
        res = self._post(
            url_path="/upsert",
            json=configuration_update.dump(),
        )
        return SearchConfig._load(res.json(), cognite_client=self._cognite_client)

    def list(self) -> SearchConfigList:
        """List all Configuration.

        Returns:
            SearchConfigList

        """
        res = self._post(url_path="/list")
        return SearchConfigList._load(res.json()["items"], cognite_client=self._cognite_client)
