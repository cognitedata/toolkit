from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient


class RoboticsAPI:
    """API for Robotics resources."""

    def __init__(self, http_client: HTTPClient) -> None:
        self.capabilities = CapabilitiesAPI(http_client)
        self.data_postprocessing = DataPostProcessingAPI(http_client)
        self.frames = FramesAPI(http_client)
        self.locations = LocationsAPI(http_client)
        self.maps = MapsAPI(http_client)
        self.robots = RobotsAPI(http_client)
