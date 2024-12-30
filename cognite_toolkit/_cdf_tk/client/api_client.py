from typing import TYPE_CHECKING

from cognite.client import ClientConfig
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from ._toolkit_client import ToolkitClient


class ToolkitAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient") -> None:
        super().__init__(config, api_version, cognite_client)
        self._toolkit_client = cognite_client
