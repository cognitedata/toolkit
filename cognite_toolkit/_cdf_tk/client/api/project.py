from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ParamRequest

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client import ToolkitClientConfig


class ProjectAPI:
    def __init__(self, config: ToolkitClientConfig) -> None:
        self._config = config
        self._http_client = HTTPClient(config, split_items_status_codes=set())

    def info(self) -> dict[str, str]:
        """Retrieve information about the current project."""
        response = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=self._config.create_api_url(""),
                parameters={"withDataModelingStatus": "true"},
                method="GET",
            )
        )
        return response.json()
