from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.project import ProjectStatusList
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ParamRequest


class ProjectAPI:
    def __init__(self, config: ToolkitClientConfig, cognite_client: CogniteClient) -> None:
        self._config = config
        self._cognite_client = cognite_client
        self._http_client = HTTPClient(config, split_items_status_codes=set())

    def status(self) -> ProjectStatusList:
        """Retrieve information about the current project."""
        response = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=f"{self._config.base_url}/api/v1/projects?withDataModelingStatus=true", method="GET"
            )
        )
        response.raise_for_status()
        body = response.get_first_body()
        return ProjectStatusList._load(body["items"], cognite_client=self._cognite_client)  # type: ignore[arg-type]
