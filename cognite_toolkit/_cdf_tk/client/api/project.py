from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.project import ProjectStatusList
from cognite_toolkit._cdf_tk.client.resource_classes.project import OrganizationResponse


class ProjectAPI:
    def __init__(self, config: ToolkitClientConfig, cognite_client: CogniteClient) -> None:
        self._config = config
        self._cognite_client = cognite_client
        self._http_client = HTTPClient(config, split_items_status_codes=set())

    def status(self) -> ProjectStatusList:
        """Retrieve information about the current project."""
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=f"{self._config.base_url}/api/v1/projects",
                method="GET",
                parameters={"withDataModelingStatus": True},
            )
        )
        success = response.get_success_or_raise()
        return ProjectStatusList._load(success.body_json["items"], cognite_client=self._cognite_client)

    def organization(self) -> OrganizationResponse:
        """Retrieve information about the organization of the current project."""
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._config.base_api_url,
                method="GET",
            )
        )
        success = response.get_success_or_raise()
        return OrganizationResponse.model_validate_json(success.body)
