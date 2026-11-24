import re
from typing import Literal
from urllib.parse import urljoin, urlparse

from cognite.client import ClientConfig
from cognite.client.credentials import CredentialProvider

from cognite_toolkit._cdf_tk.constants import DEFAULT_CLIENT_TIMEOUT


class ToolkitClientConfig(ClientConfig):
    def __init__(
        self,
        client_name: str,
        project: str,
        credentials: CredentialProvider,
        api_subversion: str | None = None,
        base_url: str | None = None,
        cluster: str | None = None,
        is_strict_validation: bool = True,
        is_sandbox: bool = False,
        headers: dict[str, str] | None = None,
        timeout: int = DEFAULT_CLIENT_TIMEOUT,
        file_transfer_timeout: int | None = None,
        debug: bool = False,
    ) -> None:
        super().__init__(
            client_name=client_name,
            project=project,
            credentials=credentials,
            api_subversion=api_subversion if api_subversion is not None else "20230101",
            base_url=base_url,
            cluster=cluster,
            headers=headers,
            timeout=timeout,
            file_transfer_timeout=file_transfer_timeout,
            debug=debug,
        )
        self.is_strict_validation = is_strict_validation
        self.is_sandbox = is_sandbox

    @classmethod
    def from_client_config(cls, config: ClientConfig, is_strict_validation: bool = True) -> "ToolkitClientConfig":
        return cls(
            client_name=config.client_name,
            project=config.project,
            credentials=config.credentials,
            api_subversion=config.api_subversion,
            base_url=config.base_url,
            cluster=config._cluster,
            headers=config.headers,
            timeout=config.timeout,
            file_transfer_timeout=config.file_transfer_timeout,
            debug=config.debug,
            is_strict_validation=is_strict_validation,
        )

    @property
    def cloud_provider(self) -> Literal["azure", "aws", "gcp", "unknown"]:
        cdf_cluster = self.attempt_to_get_cdf_cluster()
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

    def create_auth_url(self, endpoint: str) -> str:
        """Create a full Auth URL for the given endpoint.

        Args:
            endpoint (str): The Auth endpoint to append to the base URL.

        The auth host is derived from the token URL of the credentials when available,
        e.g. 'https://auth-dev.cognitedata-development.cognite.ai/oauth2/token'. Falls back
        to 'https://auth.cognite.com'.
        """
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{self._auth_base_url}/api/v1{endpoint}"

    @property
    def _auth_base_url(self) -> str:
        token_url = getattr(self.credentials, "token_url", None)
        if isinstance(token_url, str):
            parsed = urlparse(token_url)
            # Only Cognite IdP hosts (auth.*, auth-dev.*, auth-staging.*); other IdPs, e.g. Entra ID, use the default.
            if parsed.scheme and re.match(r"^auth(-[a-z0-9]+)?\.", parsed.netloc):
                return f"{parsed.scheme}://{parsed.netloc}"
        return "https://auth.cognite.com"

    def attempt_to_get_cdf_cluster(self) -> str | None:
        """A best effort attempt to extract the cluster from the base url when cluster was not explicitly provided in init."""
        if self._cluster is not None:
            return self._cluster
        return parse_cdf_cluster(self.base_url)


def parse_cdf_cluster(url: str) -> str | None:
    """Extracts the CDF cluster from a CDF URL, e.g. 'https://westeurope-1.cognitedata.com' -> 'westeurope-1'."""
    if match := re.match(r"https?://([^/\.\s]*\.plink\.)?([^/\.\s]+)\.cognitedata\.com(?::\d+)?(?:/|$)", url):
        return match.group(2)
    return None
