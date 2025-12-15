from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig

from cognite_toolkit._cdf_tk.client.api.legacy.location_filters import LocationFiltersAPI
from cognite_toolkit._cdf_tk.client.api.legacy.search_config import SearchConfigurationsAPI


class SearchAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.locations = LocationFiltersAPI(config, api_version, cognite_client)
        self.configurations = SearchConfigurationsAPI(config, api_version, cognite_client)
