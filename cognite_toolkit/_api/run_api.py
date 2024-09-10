from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import RunTransformationCommand
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class RunAPI:
    def __init__(self, toolkit_client: ToolkitClient):
        self._client = toolkit_client

    def _create_tool_config(self) -> CDFToolConfig:
        cdf_tool_config = CDFToolConfig()
        cdf_tool_config._toolkit_client = self._client
        return cdf_tool_config

    def transformation(self, external_id: str) -> bool:
        return RunTransformationCommand().run_transformation(
            self._create_tool_config(),
            external_id,
        )
