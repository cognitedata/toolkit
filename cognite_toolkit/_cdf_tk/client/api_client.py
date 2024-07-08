from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient

from .api.robotics import RoboticsAPI


class ToolkitClient(CogniteClient):
    def __init__(self, config: ClientConfig | None = None) -> None:
        super().__init__(config=config)
        self.robotics = RoboticsAPI(self._config, self._API_VERSION, self)
