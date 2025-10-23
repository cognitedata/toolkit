from typing import Literal
from urllib.parse import urljoin

from cognite.client import ClientConfig
from cognite.client.credentials import CredentialProvider


class ToolkitClientConfig(ClientConfig):
    def __init__(
        self,
        client_name: str,
        project: str,
        credentials: CredentialProvider,
        api_subversion: str | None = None,
        base_url: str | None = None,
        max_workers: int | None = None,
        is_strict_validation: bool = True,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        file_transfer_timeout: int | None = None,
        debug: bool = False,
    ) -> None:
        super().__init__(
            client_name=client_name,
            project=project,
            credentials=credentials,
            api_subversion=api_subversion,
            base_url=base_url,
            max_workers=max_workers,
            headers=headers,
            timeout=timeout,
            file_transfer_timeout=file_transfer_timeout,
            debug=debug,
        )
        self.is_strict_validation = is_strict_validation

    @classmethod
    def from_client_config(cls, config: ClientConfig, is_strict_validation: bool = True) -> "ToolkitClientConfig":
        return cls(
            client_name=config.client_name,
            project=config.project,
            credentials=config.credentials,
            api_subversion=config.api_subversion,
            base_url=config.base_url,
            max_workers=config.max_workers,
            headers=config.headers,
            timeout=config.timeout,
            file_transfer_timeout=config.file_transfer_timeout,
            debug=config.debug,
            is_strict_validation=is_strict_validation,
        )

    @property
    def cloud_provider(self) -> Literal["azure", "aws", "gcp", "unknown"]:
        cdf_cluster = self.cdf_cluster
        if cdf_cluster is None:
            return "unknown"
        elif cdf_cluster.startswith("az-") or cdf_cluster in {"azure-dev", "bluefield", "westeurope-1"}:
            return "azure"
        elif cdf_cluster.startswith("aws-") or cdf_cluster in {"orangefield"}:
            return "aws"
        elif cdf_cluster.startswith("gc-") or cdf_cluster in {
            "greenfield",
            "asia-northeast1-1",
            "cognitedata-development",
            "cognitedata-production",
        }:
            return "gcp"
        else:
            return "unknown"

    @property
    def is_private_link(self) -> bool:
        if "cognitedata.com" not in self.base_url:
            return False
        subdomain = self.base_url.split("cognitedata.com", maxsplit=1)[0]
        return "plink" in subdomain

    @property
    def base_api_url(self) -> str:
        return f"{self.base_url}/api/v1/projects/{self.project}"

    def create_api_url(self, endpoint: str) -> str:
        """Create a full API URL for the given endpoint.

        Args:
            endpoint (str): The API endpoint to append to the base URL.

        Returns:
            str: The full API URL.

        Examples:
            >>> config = ToolkitClientConfig(cluster="bluefield", project="my_project", ...)
            >>> config.create_api_url("/models/instances")
            "https://bluefield.cognitedata.com/api/v1/my_project/models/instances"
        """
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{self.base_api_url}{endpoint}"

    def create_app_url(self, endpoint: str) -> str:
        """Create a full App URL for the given endpoint.

        Args:
            endpoint (str): The App endpoint to append to the base URL.

        Returns:
            str: The full App URL.

        Examples:
            >>> config = ToolkitClientConfig(cluster="bluefield", project="my_project", ...)
            >>> config.create_app_url("/some/app/endpoint")
            "https://bluefield.cognitedata.com/apps/v1/projects/my_project/some/app/endpoint"
        """
        if not endpoint:
            raise ValueError("Endpoint must be a non-empty string")
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        base_path = f"/apps/v1/projects/{self.project}{endpoint}"
        return urljoin(self.base_url, base_path)
