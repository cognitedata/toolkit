from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.legacy.project import ProjectStatusList
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, RequestMessage2


class ProjectAPI:
    def __init__(self, config: ToolkitClientConfig, cognite_client: CogniteClient) -> None:
        self._config = config
        self._cognite_client = cognite_client
        self._http_client = HTTPClient(config, split_items_status_codes=set())

    def status(self) -> ProjectStatusList:
        """Retrieve information about the current project."""
        response = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=f"{self._config.base_url}/api/v1/projects",
                method="GET",
                parameters={"withDataModelingStatus": True},
            )
        )
        success = response.get_success_or_raise()
        return ProjectStatusList._load(success.body_json["items"], cognite_client=self._cognite_client)
