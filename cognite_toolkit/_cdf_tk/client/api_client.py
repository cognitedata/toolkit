from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._toolkit_client import ToolkitClient, ToolkitClientConfig


class ToolkitAPI:
    def __init__(self, config: "ToolkitClientConfig", toolkit_client: "ToolkitClient") -> None:
        self._config = config
        self._toolkit_client = toolkit_client
