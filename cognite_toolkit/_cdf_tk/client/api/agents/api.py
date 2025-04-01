from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api_client import APIClient

from .agents import AgentsAPI


class AtlasAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.agents = AgentsAPI(config, api_version, cognite_client)
