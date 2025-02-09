import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields
from typing import Any, Literal, TypeAlias, get_args

from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCredentials,
    OAuthDeviceCode,
    OAuthInteractive,
    Token,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.constants import TOOLKIT_CLIENT_ENTRA_ID
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitKeyError, ToolkitMissingValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._version import __version__

LoginFlow: TypeAlias = Literal["client_credentials", "token", "device_code", "interactive"]
Provider: TypeAlias = Literal["entra_id", "cdf", "other"]
VALID_LOGIN_FLOWS = get_args(LoginFlow)

CLIENT_NAME = f"CDF-Toolkit:{__version__}"
LOGIN_FLOW_DESCRIPTION = {
    "client_credentials": "Setup a service principal with client credentials",
    "interactive": "Login using the browser with your user credentials",
    "device_code": "Login using the browser with your user credentials using device code flow",
    "token": "Use a Token directly to authenticate",
}
PROVIDER_DESCRIPTION = {
    "entra_id": "Use Microsoft Entra ID to authenticate",
    "cdf": "Use Cognite IDP to authenticate",
    "other": "Use other IDP to authenticate",
}


@dataclass
class EnvOptions(Mapping):
    display_name: str
    example: str
    is_secret: bool = False

    def __getitem__(self, key: str) -> str | bool:
        return self.__dict__[key]

    def __iter__(self) -> Iterable[str]:  # type: ignore[override]
        return iter(self.__dict__.keys())

    def __len__(self) -> int:
        return len(self.__dict__)


@dataclass
class EnvironmentVariables:
    CDF_CLUSTER: str = field(metadata=EnvOptions("CDF cluster", "westeurope-1"))
    CDF_PROJECT: str = field(metadata=EnvOptions("CDF project", "publicdata"))
    CDF_URL: str | None = field(default=None, metadata=EnvOptions("CDF URL", "https://CDF_CLUSTER.cognitedata.com"))
    LOGIN_FLOW: LoginFlow = field(default="client_credentials", metadata=EnvOptions("Login flow", "client_credentials"))
    PROVIDER: Provider = field(default="entra_id", metadata=EnvOptions("Provider", "entra_id"))
    CDF_TOKEN: str | None = field(default=None, metadata=EnvOptions("OAuth2 token", example=""))
    IDP_CLIENT_ID: str | None = field(
        default=None, metadata=EnvOptions(display_name="client id", example="XXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX")
    )
    IDP_CLIENT_SECRET: str | None = field(
        default=None, metadata=EnvOptions(display_name="client secret", example="***")
    )
    IDP_TOKEN_URL: str | None = field(
        default=None,
        metadata=EnvOptions(
            display_name="token URL", example="https://login.microsoftonline.com/{IDP_TENANT_ID}/oauth2/v2.0/token"
        ),
    )
    IDP_TENANT_ID: str | None = field(
        default=None, metadata=EnvOptions(display_name="Tenant id for MS Entra", example="mytenant.onmicrosoft.com")
    )
    IDP_AUDIENCE: str | None = field(
        default=None, metadata=EnvOptions(display_name="IDP audience", example="https://{CDF_CLUSTER}.cognitedata.com")
    )
    IDP_SCOPES: str | None = field(
        default=None,
        metadata=EnvOptions(display_name="IDP scopes", example="https://{CDF_CLUSTER}.cognitedata.com/.default"),
    )
    IDP_AUTHORITY_URL: str | None = field(
        default=None,
        metadata=EnvOptions(
            display_name="IDP authority URL", example="https://login.microsoftonline.com/{IDP_TENANT_ID}"
        ),
    )
    IDP_DISCOVERY_URL: str | None = field(
        default=None,
        metadata=EnvOptions(
            display_name="IDP OIDC discovery URL (root URL excl. /.well-known/...)",
            example="https://<auth0-tenant>.auth0.com/oauth",
        ),
    )
    CDF_CLIENT_TIMEOUT: int = field(default=30, metadata=EnvOptions(display_name="CDF client timeout", example="30"))
    CDF_CLIENT_MAX_WORKERS: int = field(
        default=5, metadata=EnvOptions(display_name="CDF client max workers", example="5")
    )

    def __post_init__(self) -> None:
        self.LOGIN_FLOW = self.LOGIN_FLOW.lower()  # type: ignore[assignment]
        if self.LOGIN_FLOW not in VALID_LOGIN_FLOWS:
            raise AuthenticationError(f"Invalid login flow: {self.LOGIN_FLOW}. Valid options are {VALID_LOGIN_FLOWS}")

    # All derived properties
    @property
    def idp_token_url(self) -> str:
        if self.IDP_TOKEN_URL:
            return self.IDP_TOKEN_URL
        if self.PROVIDER == "entra_id" and self.IDP_TENANT_ID:
            return f"https://login.microsoftonline.com/{self.IDP_TENANT_ID}/oauth2/v2.0/token"
        alternative = ""
        if self.PROVIDER == "entra_id":
            alternative = " or provide IDP_TENANT_ID"
        raise ToolkitKeyError(
            f"IDP_TOKEN_URL is missing. Please provide it{alternative} in the environment variables.",
            "IDP_TOKEN_URL",
        )

    @property
    def cdf_url(self) -> str:
        return self.CDF_URL or f"https://{self.CDF_CLUSTER}.cognitedata.com"

    @property
    def idp_audience(self) -> str:
        return self.IDP_AUDIENCE or f"https://{self.CDF_CLUSTER}.cognitedata.com"

    @property
    def idp_scopes(self) -> list[str]:
        if self.IDP_SCOPES:
            return self.IDP_SCOPES.split(",")
        return [f"https://{self.CDF_CLUSTER}.cognitedata.com/.default"]

    @property
    def idp_authority_url(self) -> str:
        if self.IDP_AUTHORITY_URL:
            return self.IDP_AUTHORITY_URL
        if self.PROVIDER == "entra_id" and self.IDP_TENANT_ID:
            return f"https://login.microsoftonline.com/{self.IDP_TENANT_ID}"
        alternative = ""
        if self.PROVIDER == "entra_id":
            alternative = " or provide IDP_TENANT_ID"
        raise ToolkitKeyError(
            f"IDP_AUTHORITY_URL is missing. Please provide it{alternative} in the environment variables.",
            "IDP_AUTHORITY_URL",
        )

    @classmethod
    def create_from_environment(cls) -> "EnvironmentVariables":
        if missing := [key for key in ["CDF_CLUSTER", "CDF_PROJECT"] if key not in os.environ]:
            raise ToolkitMissingValueError(f"Missing environment variables: {humanize_collection(missing)}")
        args: dict[str, Any] = {
            field_.name: os.environ[field_.name] for field_ in fields(cls) if field_.name in os.environ
        }
        for int_key in ["CDF_CLIENT_TIMEOUT", "CDF_CLIENT_MAX_WORKERS"]:
            if int_key in args:
                args[int_key] = int(args[int_key])
        return cls(**args)

    def get_credentials(self) -> CredentialProvider:
        method_by_flow = {
            "client_credentials": self._get_oauth_client_credentials,
            "interactive": self._get_oauth_interactive,
            "device_code": self._get_oauth_device_code,
            "token": self._get_token,
        }
        if self.LOGIN_FLOW in method_by_flow:
            try:
                return method_by_flow[self.LOGIN_FLOW]()
            except KeyError as e:
                raise ToolkitMissingValueError(
                    f"The login flow '{self.LOGIN_FLOW}' requires the following environment variables: {humanize_collection(e.args[1:])}.",
                )
        raise AuthenticationError(f"Login flow {self.LOGIN_FLOW} is not supported.")

    def _get_oauth_client_credentials(self) -> OAuthClientCredentials:
        missing: list[str] = []
        if not self.IDP_CLIENT_ID:
            missing.append("IDP_CLIENT_ID")
        if not self.IDP_CLIENT_SECRET:
            missing.append("IDP_CLIENT_SECRET")
        if missing:
            raise ToolkitKeyError(f"Missing environment variables: {humanize_collection(missing)}", *missing)
        if self.PROVIDER == "cdf":
            return OAuthClientCredentials(
                client_id=self.IDP_CLIENT_ID,  # type: ignore[arg-type]
                client_secret=self.IDP_CLIENT_SECRET,  # type: ignore[arg-type]
                token_url=self.idp_token_url,
                scopes=None,  # type: ignore[arg-type]
            )
        return OAuthClientCredentials(
            client_id=self.IDP_CLIENT_ID,  # type: ignore[arg-type]
            client_secret=self.IDP_CLIENT_SECRET,  # type: ignore[arg-type]
            token_url=self.idp_token_url,
            audience=self.idp_audience,
            scopes=self.idp_scopes,
        )

    def _get_oauth_interactive(self) -> OAuthInteractive:
        if not self.IDP_CLIENT_ID:
            raise ToolkitKeyError("Missing environment variables: IDP_CLIENT_ID", "IDP_CLIENT_ID")
        return OAuthInteractive(
            client_id=self.IDP_CLIENT_ID,
            authority_url=self.idp_authority_url,
            scopes=self.idp_scopes,
        )

    def _get_oauth_device_code(self) -> OAuthDeviceCode:
        if self.PROVIDER == "entra_id":
            if not self.IDP_TENANT_ID:
                raise ToolkitKeyError("Missing environment variables: IDP_TENANT_ID", "IDP_TENANT_ID")
            # TODO: If the user has submitted the wrong scopes, we may get a valid token that gives 401 on the CDF API.
            # The user will then have to wait until the token has expired to retry with the correct scopes.
            # If we add clear_cache=True to the OAuthDeviceCode, the token cache will be cleared.
            # We could add a cli option to auth verify, e.g. --clear-token-cache, that will clear the cache.
            return OAuthDeviceCode.default_for_azure_ad(
                tenant_id=self.IDP_TENANT_ID,
                client_id=TOOLKIT_CLIENT_ENTRA_ID,
                cdf_cluster=self.CDF_CLUSTER,
                clear_cache=False,
            )
        elif self.PROVIDER == "other":
            missing: list[str] = []
            if not self.IDP_DISCOVERY_URL:
                missing.append("IDP_DISCOVERY_URL")
            if not self.IDP_CLIENT_ID:
                missing.append("IDP_CLIENT_ID")
            if missing:
                raise ToolkitKeyError(f"Missing environment variables: {humanize_collection(missing)}", *missing)

            return OAuthDeviceCode(
                authority_url=None,
                cdf_cluster=self.CDF_CLUSTER,
                oauth_discovery_url=self.IDP_DISCOVERY_URL,
                client_id=self.IDP_CLIENT_ID,  # type: ignore[arg-type]
                audience=self.idp_audience,
            )
        else:
            raise AuthenticationError(f"The provider {self.PROVIDER} is not supported for device code flow.")

    def _get_token(self) -> Token:
        if not self.CDF_TOKEN:
            raise ToolkitKeyError("CDF_TOKEN must be set in the environment", "CDF_TOKEN")
        return Token(self.CDF_TOKEN)

    def get_config(self) -> ToolkitClientConfig:
        return ToolkitClientConfig(
            client_name=CLIENT_NAME,
            project=self.CDF_PROJECT,
            credentials=self.get_credentials(),
            base_url=self.cdf_url,
            timeout=self.CDF_CLIENT_TIMEOUT,
            max_workers=self.CDF_CLIENT_MAX_WORKERS,
        )

    def get_client(self) -> ToolkitClient:
        return ToolkitClient(config=self.get_config())

    @classmethod
    def dump_environment_variables(cls, include_os: bool = True) -> dict[str, Any]:
        global _SINGLETON
        if _SINGLETON is None:
            _SINGLETON = cls.create_from_environment()
        raise NotImplementedError("include derived properties")
        # variables = {key:  for key in fields(_SINGLETON) if value := getattr(_SINGLETON, key)}
        # if include_os:
        #     variables.update(os.environ)
        # return variables


_SINGLETON: EnvironmentVariables | None = None
