import builtins

from cognite_toolkit._cdf_tk.client.cdf_client import ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import SimulatorResponse

from .simulator_integrations import SimulatorIntegrationsAPI
from .simulator_logs import SimulatorLogsAPI
from .simulator_model_revisions import SimulatorModelRevisionsAPI
from .simulator_models import SimulatorModelsAPI
from .simulator_routine_revisions import SimulatorRoutineRevisionsAPI
from .simulator_routines import SimulatorRoutinesAPI
from .simulator_runs import SimulatorRunsAPI

_SIMULATORS_LIST_LIMIT = 1000


class SimulatorsAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self.models = SimulatorModelsAPI(http_client)
        self.model_revisions = SimulatorModelRevisionsAPI(http_client)
        self.routines = SimulatorRoutinesAPI(http_client)
        self.routine_revisions = SimulatorRoutineRevisionsAPI(http_client)
        self.runs = SimulatorRunsAPI(http_client)
        self.integrations = SimulatorIntegrationsAPI(http_client)
        self.logs = SimulatorLogsAPI(http_client)

    def list(self, limit: int = _SIMULATORS_LIST_LIMIT) -> builtins.list[SimulatorResponse]:
        """List the simulators in the project. The endpoint returns at most 1000 simulators.

        Args:
            limit: Maximum number of simulators to return.

        Returns:
            The simulators.
        """
        if not 0 < limit <= _SIMULATORS_LIST_LIMIT:
            raise ValueError(f"Limit must be between 1 and {_SIMULATORS_LIST_LIMIT}, got {limit}.")
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url("/simulators/list"),
            method="POST",
            body_content={"limit": limit},
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return ResponseItems[SimulatorResponse].model_validate_json(response.body).items
