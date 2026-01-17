from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .robotics_capabilities import CapabilitiesAPI
from .robotics_data_postprocessing import DataPostProcessingAPI
from .robotics_frames import FramesAPI
from .robotics_locations import LocationsAPI
from .robotics_maps import MapsAPI
from .robotics_robots import RobotsAPI


class RoboticsAPI:
    """API for Robotics resources."""

    def __init__(self, http_client: HTTPClient) -> None:
        self.frames = FramesAPI(http_client)
        self.robots = RobotsAPI(http_client)
        self.maps = MapsAPI(http_client)
        self.data_postprocessing = DataPostProcessingAPI(http_client)
        self.capabilities = CapabilitiesAPI(http_client)
        self.locations = LocationsAPI(http_client)
