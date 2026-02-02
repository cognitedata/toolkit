from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .simulator_model_revisions import SimulatorModelRevisionsAPI
from .simulator_models import SimulatorModelsAPI
from .simulator_routine_revisions import SimulatorRoutineRevisionsAPI
from .simulator_routines import SimulatorRoutinesAPI


class SimulatorsAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.models = SimulatorModelsAPI(http_client)
        self.model_revisions = SimulatorModelRevisionsAPI(http_client)
        self.routines = SimulatorRoutinesAPI(http_client)
        self.routine_revisions = SimulatorRoutineRevisionsAPI(http_client)
