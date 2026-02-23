from functools import lru_cache

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.project import OrganizationResponse, ProjectStatusList


class ProjectAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def status(self) -> ProjectStatusList:
        """Retrieve information about the current project."""
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=f"{self._http_client.config.base_url}/api/v1/projects",
                method="GET",
                parameters={"withDataModelingStatus": True},
            )
        ).get_success_or_raise()
        result = ProjectStatusList.model_validate_json(response.body)
        result._project = self._http_client.config.project
        return result

    def organization(self) -> OrganizationResponse:
        """Retrieve information about the organization of the current project."""
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._http_client.config.base_api_url,
                method="GET",
            )
        )
        success = response.get_success_or_raise()
        return OrganizationResponse.model_validate_json(success.body)

    @lru_cache(maxsize=1)
    def get_organization_id(self) -> str:
        """Retrieve the organization id of the current project."""
        return self.organization().organization
