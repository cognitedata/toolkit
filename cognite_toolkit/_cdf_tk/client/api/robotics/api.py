from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api_client import APIClient

from .capabilities import CapabilitiesAPI
from .data_postprocessing import DataPostProcessingAPI
from .frames import FramesAPI
from .locations import LocationsAPI
from .maps import MapsAPI
from .robots import RobotsAPI


class RoboticsAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.robots = RobotsAPI(config, api_version, cognite_client)
        self.data_postprocessing = DataPostProcessingAPI(config, api_version, cognite_client)
        self.locations = LocationsAPI(config, api_version, cognite_client)
        self.frames = FramesAPI(config, api_version, cognite_client)
        self.maps = MapsAPI(config, api_version, cognite_client)
        self.capabilities = CapabilitiesAPI(config, api_version, cognite_client)
