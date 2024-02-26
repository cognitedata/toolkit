from __future__ import annotations

from cognite.client import CogniteClient

from cognite_toolkit._api.modules_api import ModulesAPI


class CogniteToolkit:
    def __init__(self, client: CogniteClient, url: str | None = None):
        self.modules = ModulesAPI(client, url)
