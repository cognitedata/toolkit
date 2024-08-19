from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api_client import APIClient

from cognite_toolkit._cdf_tk.client.api.locations.location_filters import LocationFiltersAPI


class LocationsAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.location_filters = LocationFiltersAPI(config, api_version, cognite_client)
