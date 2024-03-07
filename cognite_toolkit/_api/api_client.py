from __future__ import annotations

from cognite_toolkit._api.modules_api import ModulesAPI
from cognite_toolkit._api.run_api import RunAPI
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class CogniteToolkit:
    def __init__(self, url: str | None = None):
        self.client = CDFToolConfig().client
        self.modules = ModulesAPI(self.client.config.project, url)
        self.run = RunAPI(self.client)
