from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.client.data_classes.project import ProjectStatus
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ParamRequest

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client import ToolkitClientConfig


class ProjectAPI:
    def __init__(self, config: "ToolkitClientConfig") -> None:
        self._config = config
        self._http_client = HTTPClient(config, split_items_status_codes=set())

    def status(self) -> ProjectStatus:
        """Retrieve information about the current project."""
        response = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=f"{self._config.base_url}/api/v1/projects?withDataModelingStatus=true", method="GET"
            )
        )
        response.raise_for_status()
        body = response.get_first_body()
        # We expect the API will always return exactly one project when querying by project name
        return ProjectStatus._load(body["items"][0])  # type: ignore[index,arg-type]
