from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .simulator_models import SimulatorModelsAPI


class SimulatorsAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.models = SimulatorModelsAPI(http_client)
