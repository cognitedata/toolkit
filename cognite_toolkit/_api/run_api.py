import json

from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.run import run_function, run_transformation
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class RunAPI:
    def __init__(self, cognite_client: CogniteClient):
        self._client = cognite_client

    def _create_tool_config(self) -> CDFToolConfig:
        cluster = self._client.config.base_url.removeprefix("https://").split(".", maxsplit=1)[0]
        cdf_tool_config = CDFToolConfig(cluster=cluster, project=self._client.config.project)
        cdf_tool_config._client = self._client
        return cdf_tool_config

    def transformation(self, external_id: str) -> bool:
        return run_transformation(
            self._create_tool_config(),
            external_id,
        )

    def function(self, external_id: str, payload: dict, follow: bool = False) -> bool:
        return run_function(
            self._create_tool_config(),
            external_id,
            json.dumps(payload),
            follow,
        )
