from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient

from .api.dml import DMLAPI
from .api.location_filters import LocationFiltersAPI
from .api.robotics import RoboticsAPI


class ToolkitClient(CogniteClient):
    def __init__(self, config: ClientConfig | None = None) -> None:
        super().__init__(config=config)
        self.location_filters = LocationFiltersAPI(self._config, self._API_VERSION, self)
        self.robotics = RoboticsAPI(self._config, self._API_VERSION, self)
        self.dml = DMLAPI(self._config, self._API_VERSION, self)
