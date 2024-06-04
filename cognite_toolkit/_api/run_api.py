import json

from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.commands import RunFunctionCommand, RunTransformationCommand
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class RunAPI:
    def __init__(self, cognite_client: CogniteClient):
        self._client = cognite_client

    def _create_tool_config(self) -> CDFToolConfig:
        cdf_tool_config = CDFToolConfig()
        cdf_tool_config._client = self._client
        return cdf_tool_config

    def transformation(self, external_id: str) -> bool:
        return RunTransformationCommand().run_transformation(
            self._create_tool_config(),
            external_id,
        )

    def function(self, external_id: str, payload: dict, follow: bool = False) -> bool:
        return RunFunctionCommand().run_function(
            self._create_tool_config(),
            external_id,
            json.dumps(payload),
            follow,
        )
