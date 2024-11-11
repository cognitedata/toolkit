# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import itertools
import json
import os
import shutil
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

import questionary
import typer
from cognite.client import ClientConfig
from cognite.client.config import global_config
from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCredentials,
    OAuthDeviceCode,
    OAuthInteractive,
    Token,
)
from cognite.client.data_classes import ClientCredentials
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataSetsAcl,
    ExtractionPipelinesAcl,
    LocationFiltersAcl,
    SecurityCategoriesAcl,
    TimeSeriesAcl,
)
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich import print
from rich.prompt import Prompt

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    TOOLKIT_CLIENT_ENTRA_ID,
    URL,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ResourceRetrievalError,
    ToolkitResourceMissingError,
)
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._version import __version__

if TYPE_CHECKING:
    pass


LoginFlow: TypeAlias = Literal["client_credentials", "token", "device_code", "interactive"]
Provider: TypeAlias = Literal["entra_id", "other"]

LOGIN_FLOW_DESCRIPTION = {
    "client_credentials": "Setup a service principal with client credentials",
    "interactive": "Login using the browser with your user credentials",
    "device_code": "Login using the browser with your user credentials using device code flow",
    "token": "Use a Token directly to authenticate",
}

PROVDER_DESCRIPTION = {
    "entra_id": "Use Microsoft Entra ID to authenticate",
    "other": "Use other IDP to authenticate",
}


@dataclass
class AuthVariables:
    cluster: str | None = field(
        metadata=dict(env_name="CDF_CLUSTER", display_name="CDF cluster", example="westeurope-1")
    )
    project: str | None = field(metadata=dict(env_name="CDF_PROJECT", display_name="CDF project", example="publicdata"))
    cdf_url: str | None = field(
        default=None,
        metadata=dict(env_name="CDF_URL", display_name="CDF URL", example="https://CDF_CLUSTER.cognitedata.com"),
    )
    login_flow: LoginFlow = field(
        default="client_credentials",
        metadata=dict(
            env_name="LOGIN_FLOW",
            display_name="Login flow",
            example="client_credentials",
        ),
    )
    provider: Provider = field(
        default="entra_id",
        metadata=dict(
            env_name="PROVIDER",
            display_name="Provider",
            example="entra_id",
        ),
    )
    token: str | None = field(
        default=None, metadata=dict(env_name="CDF_TOKEN", display_name="OAuth2 token", example="")
    )
    client_id: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_CLIENT_ID", display_name="client id", example="XXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        ),
    )
    client_secret: str | None = field(
        default=None,
        metadata=dict(env_name="IDP_CLIENT_SECRET", display_name="client secret", example=""),
    )
    token_url: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_TOKEN_URL",
            display_name="token URL",
            example="https://login.microsoftonline.com/IDP_TENANT_ID/oauth2/v2.0/token",
        ),
    )
    tenant_id: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_TENANT_ID",
            display_name="Tenant id for MS Entra",
            example="12345678-1234-1234-1234-123456789012",
        ),
    )
    audience: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_AUDIENCE",
            display_name="IDP audience",
            example="https://CDF_CLUSTER.cognitedata.com",
        ),
    )
    scopes: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_SCOPES",
            display_name="IDP scopes",
            example="https://CDF_CLUSTER.cognitedata.com/.default",
        ),
    )
    authority_url: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_AUTHORITY_URL",
            display_name="IDP authority URL",
            example="https://login.microsoftonline.com/IDP_TENANT_ID",
        ),
    )
    oidc_discovery_url: str | None = field(
        default=None,
        metadata=dict(
            env_name="IDP_DISCOVERY_URL",
            display_name="IDP OIDC discovery URL (root URL excl. /.well-known/...)",
            example="https://<auth0-tenant>.auth0.com/oauth",
        ),
    )

    def __post_init__(self) -> None:
        # Set defaults based on cluster and tenant_id
        if self.cluster:
            self.set_cluster_defaults()
        if self.tenant_id:
            self.set_token_id_defaults()
        if self.token and self.login_flow != "token":
            print(
                f"  [bold yellow]Warning[/] CDF_TOKEN detected. This will override LOGIN_FLOW, "
                f"thus LOGIN_FLOW={self.login_flow} will be ignored"
            )
            self.login_flow = "token"

    def set_token_id_defaults(self) -> None:
        if self.tenant_id:
            self.token_url = self.token_url or f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            self.authority_url = self.authority_url or f"https://login.microsoftonline.com/{self.tenant_id}"

    def set_cluster_defaults(self) -> None:
        if self.cluster:
            self.cdf_url = self.cdf_url or f"https://{self.cluster}.cognitedata.com"
            self.audience = self.audience or f"https://{self.cluster}.cognitedata.com"
            self.scopes = self.scopes or f"https://{self.cluster}.cognitedata.com/.default"

    @property
    def is_complete(self) -> bool:
        if self.cdf_url is None:
            return False
        if self.login_flow == "token":
            return self.token is not None
        elif self.login_flow == "interactive":
            return self.client_id is not None and self.tenant_id is not None and self.scopes is not None
        elif self.login_flow == "client_credentials":
            return (
                self.client_id is not None
                and self.client_secret is not None
                and self.token_url is not None
                and self.scopes is not None
                and self.audience is not None
            )
        elif self.login_flow == "device_code" and self.provider == "entra_id":
            return self.tenant_id is not None
        elif self.login_flow == "device_code" and self.provider == "other":
            return self.client_id is not None and self.oidc_discovery_url is not None
        return False

    @classmethod
    def from_env(cls, override: dict[str, Any] | None = None) -> AuthVariables:
        override = override or {}
        args: dict[str, Any] = {}
        for field_ in fields(cls):
            if env_name := field_.metadata.get("env_name"):
                args[field_.name] = override.get(env_name, os.environ.get(env_name))
        return cls(**args)

    def create_dotenv_file(self) -> str:
        lines = [
            "# .env file generated by cognite-toolkit",
            self._write_var("login_flow"),
            self._write_var("cluster"),
            self._write_var("project"),
        ]
        if self.login_flow == "token":
            lines += [
                "# When using a token, the IDP variables are not needed, so they are not included.",
                self._write_var("token"),
            ]
        elif self.login_flow == "client_credentials":
            lines += [
                self._write_var("client_id"),
                self._write_var("client_secret"),
            ]
        elif self.login_flow == "device_code":
            lines += [
                self._write_var("provider"),
            ]
        elif self.login_flow == "interactive":
            lines += [
                self._write_var("client_id"),
            ]
        else:
            raise ValueError(f"Login flow {self.login_flow} is not supported.")
        if self.login_flow in ("client_credentials", "interactive"):
            lines += [
                "# Note: Either the TENANT_ID or the TENANT_URL must be written.",
                self._write_var("tenant_id"),
                self._write_var("token_url"),
            ]
        elif self.login_flow == "device_code" and self.provider == "entra_id":
            lines += [
                self._write_var("tenant_id"),
            ]
        elif self.login_flow == "device_code" and self.provider == "other":
            lines += [
                self._write_var("client_id"),
                self._write_var("oidc_discovery_url"),
            ]
        lines += [
            "# The below variables are the defaults, they are automatically constructed unless they are set.",
            self._write_var("cdf_url"),
        ]
        if self.login_flow in ("client_credentials", "interactive"):
            lines += [
                self._write_var("scopes"),
            ]
        if self.login_flow == "interactive":
            lines += [
                self._write_var("authority_url"),
            ]
        if self.login_flow in ("client_credentials") or (self.login_flow == "device_code" and self.provider == "other"):
            lines += [
                self._write_var("audience"),
            ]

        return "\n".join(lines)

    def _write_var(self, var_name: str) -> str:
        value = getattr(self, var_name)
        field_ = _auth_field_by_name[var_name].metadata
        if value is None:
            return f"{field_['env_name']}="
        return f"{field_['env_name']}={value}"


class AuthReader:
    """Reads and validate the auth variables

    Args:
        auth_vars (AuthVariables): The auth variables to validate
        verbose (bool): If True, print additional information
        skip_prompt (bool): If True, skip prompting the user for input
            and only do the validation.

    """

    def __init__(self, auth_vars: AuthVariables, verbose: bool, skip_prompt: bool = False):
        self.auth_vars = auth_vars
        self.status: Literal["ok", "warning"] = "ok"
        self.messages: list[str] = []
        self.verbose = verbose
        self.skip_prompt = skip_prompt

    def from_user(self) -> AuthVariables:
        auth_vars = self.auth_vars
        login_flow = questionary.select(
            "Choose the login flow",
            choices=[
                Choice(title=f"{flow}: {description}", value=flow)
                for flow, description in LOGIN_FLOW_DESCRIPTION.items()
            ],
        ).ask()
        auth_vars.login_flow = login_flow
        print("Default values in parentheses. Press Enter to keep current value")
        auth_vars.cluster = self.prompt_user("cluster")
        auth_vars.project = self.prompt_user("project")
        auth_vars.set_cluster_defaults()
        if not (auth_vars.cluster and auth_vars.project):
            missing = [field for field in ["cluster", "project"] if not getattr(self, field)]
            raise AuthenticationError(f"CDF Cluster and project are required. Missing: {', '.join(missing)}.")
        if login_flow == "token":
            auth_vars.token = self.prompt_user("token")
        elif login_flow == "client_credentials":
            auth_vars.client_id = self.prompt_user("client_id")
            if new_secret := self.prompt_user("client_secret", password=True):
                auth_vars.client_secret = new_secret
            else:
                print("  Keeping existing client secret.")
        elif login_flow == "interactive":
            auth_vars.client_id = self.prompt_user("client_id")
        elif login_flow == "device_code":
            provider = questionary.select(
                "Choose the provider",
                choices=[
                    Choice(title=f"{provider}: {description}", value=provider)
                    for provider, description in PROVDER_DESCRIPTION.items()
                ],
            ).ask()
            auth_vars.provider = provider

        if login_flow in ("client_credentials", "interactive") or (
            login_flow == "device_code" and auth_vars.provider == "entra_id"
        ):
            auth_vars.tenant_id = self.prompt_user("tenant_id")
            auth_vars.set_token_id_defaults()
        elif login_flow == "device_code" and auth_vars.provider == "other":
            auth_vars.client_id = self.prompt_user("client_id")
            auth_vars.oidc_discovery_url = self.prompt_user("oidc_discovery_url")

        default_variables = ["cdf_url"]
        if login_flow == "client_credentials":
            default_variables.extend(["scopes", "audience"])
        elif login_flow == "interactive":
            default_variables.extend(["scopes", "authority_url"])
        print("The below variables are the defaults,")
        for field_name in default_variables:
            current_value = getattr(self.auth_vars, field_name)
            metadata = _auth_field_by_name[field_name].metadata
            print(f"  {metadata['env_name']}={current_value}")
        if questionary.confirm("Do you want to change any of these variables?", default=False).ask():
            for field_name in default_variables:
                setattr(auth_vars, field_name, self.prompt_user(field_name))

        new_env_file = auth_vars.create_dotenv_file()
        if Path(".env").exists():
            existing = Path(".env").read_text()
            if existing == new_env_file:
                print("Identical '.env' file already exist.")
                return auth_vars
            MediumSeverityWarning("'.env' file already exists").print_warning()
            filename = next(f"backup_{no}.env" for no in itertools.count() if not Path(f"backup_{no}.env").exists())

            if questionary.confirm(
                f"Do you want to overwrite the existing '.env' file? The existing will be renamed to {filename}",
                default=False,
            ).ask():
                shutil.move(".env", filename)
                Path(".env").write_text(new_env_file)
        elif questionary.confirm("Do you want to save these to .env file for next time?", default=True).ask():
            Path(".env").write_text(new_env_file)

        return auth_vars

    def prompt_user(
        self,
        field_name: str,
        choices: list[str] | None = None,
        password: bool | None = None,
        expected: str | None = None,
    ) -> str | None:
        try:
            current_value = getattr(self.auth_vars, field_name)
            field_ = _auth_field_by_name[field_name]
            metadata = field_.metadata
            example = (
                metadata["example"]
                .replace("CDF_CLUSTER", self.auth_vars.cluster or "<cluster>")
                .replace("IDP_TENANT_ID", self.auth_vars.tenant_id or "<tenant_id>")
            )
            display_name = metadata["display_name"]
            default = current_value or (field_.default if isinstance(field_.default, str) else None)
        except KeyError as e:
            raise RuntimeError("AuthVariables not created correctly. Contact Support") from e

        extra_args: dict[str, Any] = {}
        if not password:
            extra_args["default"] = default
        else:
            extra_args["password"] = True
        if choices:
            extra_args["choices"] = choices

        if password and current_value:
            prompt = f"You have set {display_name}, change it?"
        elif example == default or (not example):
            prompt = f"{display_name}?"
        else:
            prompt = f"{display_name}, e.g., [italic]{example}[/]?"

        response: str | None
        if self.skip_prompt:
            response = default
        else:
            response = Prompt.ask(prompt, **extra_args)
        if not expected or response == expected:
            if isinstance(response, str) and self.verbose:
                self.messages.append(f"  {display_name}={response} is set correctly.")
            elif response is None:
                self.messages.append(f"  {display_name} is not set.")
            return response
        self.messages.append(
            f"[bold yellow]WARNING[/]: {display_name} is set to {response}, are you sure it shouldn't be {expected}?"
        )
        self.status = "warning"
        return response


_auth_field_by_name = {field.name: field for field in fields(AuthVariables)}


class CDFToolConfig:
    """Configurations for how to store data in CDF

    Properties:
        toolkit_client: active ToolkitClient
    Functions:
        verify_client: verify that the client has correct credentials and specified access capabilities
        verify_dataset: verify that the data set exists and that the client has access to it

    """

    @dataclass
    class _Cache:
        existing_spaces: set[str] = field(default_factory=set)
        data_set_id_by_external_id: dict[str, int] = field(default_factory=dict)
        extraction_pipeline_id_by_external_id: dict[str, int] = field(default_factory=dict)
        security_categories_by_name: dict[str, int] = field(default_factory=dict)
        asset_id_by_external_id: dict[str, int] = field(default_factory=dict)
        timeseries_id_by_external_id: dict[str, int] = field(default_factory=dict)
        locationfilter_id_by_external_id: dict[str, int] = field(default_factory=dict)
        token_inspect: TokenInspection | None = None

    def __init__(
        self,
        token: str | None = None,
        cluster: str | None = None,
        project: str | None = None,
        cdf_url: str | None = None,
        skip_initialization: bool = False,
    ) -> None:
        self._cache = self._Cache()
        self._environ: dict[str, str | None] = {}
        # If cluster, project, or token are passed as arguments, we override the environment variables.
        # This means these will be used when we initialize the CogniteClient when we initialize from
        # environment variables. Note if we are running in the browser, we will not use these arguments.
        if project:
            self._environ["CDF_PROJECT"] = project
        if cluster:
            self._environ["CDF_CLUSTER"] = cluster
        if token:
            self._environ["CDF_TOKEN"] = token
        if cdf_url:
            self._environ["CDF_URL"] = cdf_url

        # ClientName is used for logging usage of the CDF-Toolkit.
        self._client_name = f"CDF-Toolkit:{__version__}"

        self._cluster: str | None = cluster
        self._project: str | None = project
        self._cdf_url: str | None = None
        self._scopes: list[str] = []
        self._audience: str | None = None
        self._token_url: str | None = None
        self._credentials_args: dict[str, Any] = {}
        self._credentials_provider: CredentialProvider | None = None
        self._toolkit_client: ToolkitClient | None = None

        global_config.disable_pypi_version_check = True
        global_config.silence_feature_preview_warnings = True
        if _RUNNING_IN_BROWSER:
            self._initialize_in_browser()
            return

        self._auth_vars = AuthVariables.from_env(self._environ)
        if not skip_initialization:
            self.initialize_from_auth_variables(self._auth_vars)
        self._login_flow = self._auth_vars.login_flow

    def _initialize_in_browser(self) -> None:
        try:
            self._toolkit_client = ToolkitClient()
        except CogniteAPIError as e:
            raise AuthenticationError(f"Failed to initialize CogniteClient in browser: {e}")

        if self._cluster or self._project:
            print("[bold yellow]Warning[/] Cluster and project are arguments ignored when running in the browser.")
        self._cluster = self._toolkit_client.config.base_url.removeprefix("https://").split(".", maxsplit=1)[0]
        self._project = self._toolkit_client.config.project
        self._cdf_url = self._toolkit_client.config.base_url

    def initialize_from_auth_variables(self, auth: AuthVariables, clear_cache: bool = False) -> None:
        """Initialize the CDFToolConfig from the AuthVariables and returns whether it was successful or not."""
        cluster = auth.cluster or self._cluster
        project = auth.project or self._project

        if cluster is None or project is None:
            raise AuthenticationError("Cluster and Project must be set to authenticate the client.")

        self._cluster = cluster
        self._project = project
        self._cdf_url = auth.cdf_url or self._cdf_url

        if auth.login_flow == "token":
            if not auth.token:
                raise AuthenticationError("Login flow=token is set but no CDF_TOKEN is not provided.")
            self._credentials_args = dict(token=auth.token)
            self._credentials_provider = Token(**self._credentials_args)
        elif auth.login_flow == "device_code" and auth.provider == "entra_id":
            # TODO: If the user has submitted the wrong scopes, we may get a valid token that gives 401 on the CDF API.
            # The user will then have to wait until the token has expired to retry with the correct scopes.
            # If we add clear_cache=True to the OAuthDeviceCode, the token cache will be cleared.
            # We could add a cli option to auth verify, e.g. --clear-token-cache, that will clear the cache.
            if not auth.tenant_id:
                raise ValueError("IDP_TENANT_ID is required for device code login.")
            # For Entra ID, we have defaults for everything, even the app registration as we can use the CDF public app.
            self._credentials_args = dict(
                tenant_id=auth.tenant_id,
                client_id=TOOLKIT_CLIENT_ENTRA_ID,
                cdf_cluster=self._cluster,
                clear_cache=clear_cache,
            )
            self._credentials_provider = OAuthDeviceCode.default_for_azure_ad(**self._credentials_args)
        elif auth.login_flow == "device_code" and auth.provider == "other":
            if not auth.client_id:
                raise ValueError("IDP_CLIENT_ID is required for device code login.")
            self._credentials_args = dict(
                authority_url=None,
                cdf_cluster=auth.cluster,
                oauth_discovery_url=auth.oidc_discovery_url,
                client_id=auth.client_id,
                audience=auth.audience,
                clear_cache=clear_cache,
            )
            self._credentials_provider = OAuthDeviceCode(**self._credentials_args)
        elif auth.login_flow == "interactive":
            if auth.scopes:
                self._scopes = [auth.scopes]
            if not (auth.client_id and auth.authority_url and auth.scopes):
                raise AuthenticationError(
                    "Login flow=interactive is set but missing required authentication "
                    "variables: IDP_CLIENT_ID and IDP_TENANT_ID (or IDP_AUTHORITY_URL). Cannot authenticate the client."
                )
            self._credentials_args = dict(
                authority_url=auth.authority_url,
                client_id=auth.client_id,
                scopes=self._scopes,
            )
            self._credentials_provider = OAuthInteractive(**self._credentials_args)
        elif auth.login_flow == "client_credentials" or auth.login_flow is None:
            if auth.login_flow is None:
                print(
                    "  [bold yellow]Warning[/] No login flow is set. Defaulting to client_credentials. "
                    "Set LOGIN_FLOW to 'client_credentials', 'token', or 'interactive' to avoid this warning."
                )
            if auth.scopes:
                self._scopes = [auth.scopes]
            if auth.audience:
                self._audience = auth.audience

            if not (auth.token_url and auth.client_id and auth.client_secret and self._scopes and self._audience):
                raise AuthenticationError(
                    "Login flow=client_credentials is set but missing required authentication "
                    "variables: IDP_CLIENT_ID, IDP_CLIENT_SECRET and IDP_TENANT_ID (or IDP_TOKEN_URL). "
                    "Cannot authenticate the client."
                )
            self._credentials_args = dict(
                token_url=auth.token_url,
                client_id=auth.client_id,
                client_secret=auth.client_secret,
                scopes=self._scopes,
                audience=self._audience,
            )
            self._credentials_provider = OAuthClientCredentials(**self._credentials_args)
        else:
            raise AuthenticationError(f"Login flow {auth.login_flow} is not supported.")
        self._token_url = auth.token_url
        self._toolkit_client = ToolkitClient(
            ClientConfig(
                client_name=self._client_name,
                base_url=self._cdf_url,
                project=self._project,
                credentials=self._credentials_provider,
            )
        )
        self._update_environment_variables()
        self._auth_vars = auth

    def _update_environment_variables(self) -> None:
        """This updates the cache environment variables with the auth
        variables.

        This is necessary for the .as_string() method to dump correctly.
        """
        for field_name in ["cluster", "project", "cdf_url", "scopes", "audience"]:
            try:
                field_ = _auth_field_by_name[field_name]
                env_name = field_.metadata["env_name"]
                value = getattr(self, f"_{field_name}")
            except KeyError as e:
                # This means that the attribute is not set correctly in AuthVariables,
                # ensure that 'env_name' is set in the metadata for all fields in AuthVariables.
                raise RuntimeError("AuthVariables not created correctly. Contact Support") from e

            if value:
                self._environ[env_name] = value[0] if isinstance(value, list) else value

    @classmethod
    def from_context(cls, ctx: typer.Context) -> CDFToolConfig:
        if ctx.obj.mockToolGlobals is not None:
            return ctx.obj.mockToolGlobals
        else:
            return CDFToolConfig()

    def environment_variables(self) -> dict[str, str | None]:
        return {**self._environ.copy(), **os.environ}

    def as_string(self) -> str:
        environment = self._environ.copy()
        if "IDP_CLIENT_SECRET" in environment:
            environment["IDP_CLIENT_SECRET"] = "***"
        if "TRANSFORMATIONS_CLIENT_SECRET" in environment:
            environment["TRANSFORMATIONS_CLIENT_SECRET"] = "***"
        envs = ""
        for e in environment:
            envs += f"  {e}={environment[e]}\n"
        return f"CDF Project {self._project} in cluster {self._cluster}:\n{envs}"

    def __str__(self) -> str:
        environment = self._environ.copy()
        if "IDP_CLIENT_SECRET" in environment:
            environment["IDP_CLIENT_SECRET"] = "***"
        if "TRANSFORMATIONS_CLIENT_SECRET" in environment:
            environment["TRANSFORMATIONS_CLIENT_SECRET"] = "***"
        return f"Cluster {self._cluster} with project {self._project} and config:\n" + json.dumps(
            environment, indent=2, sort_keys=True
        )

    @property
    def toolkit_client(self) -> ToolkitClient:
        if self._toolkit_client is None:
            raise ValueError("ToolkitClient is not initialized.")
        return self._toolkit_client

    @property
    def project(self) -> str:
        if self._project is None:
            raise ValueError("Project is not initialized.")
        return self._project

    @overload
    def environ(self, attr: str, default: str | None = None, fail: Literal[True] = True) -> str: ...

    @overload
    def environ(self, attr: str, default: str | None = None, fail: Literal[False] = False) -> str | None: ...

    def environ(self, attr: str, default: str | None = None, fail: bool = True) -> str | None:
        """Helper function to load variables from the environment.

        Use python-dotenv to load environment variables from an .env file before
        using this function.

        If the environment variable has spaces, it will be split into a list of strings.

        Args:
            attr: name of environment variable
            default: default value if environment variable is not set
            fail: if True, raise ValueError if environment variable is not set

        Yields:
            Value of the environment variable
            Raises ValueError if environment variable is not set and fail=True
        """
        if value := self._environ.get(attr):
            return value
        # If the var was none, we want to re-evaluate from environment.
        var: str | None = os.environ.get(attr)
        if var is None and default is None and fail:
            raise ValueError(f"{attr} property is not available as an environment variable and no default set.")
        elif var is None and default is None:
            # Todo: Should this be handled differently?
            var = None
        elif var is None:
            var = default

        self._environ[attr] = var
        return var

    @property
    def _token_inspection(self) -> TokenInspection:
        if self._cache.token_inspect is None:
            try:
                self._cache.token_inspect = self.toolkit_client.iam.token.inspect()
            except CogniteAPIError as e:
                raise AuthorizationError(
                    f"Don't seem to have any access rights. {e}\n"
                    f"Please visit [link={URL.configure_access}]the documentation[/link] "
                    f"and ensure you have configured your access correctly."
                ) from e
        return self._cache.token_inspect

    def verify_authorization(
        self, capabilities: Capability | Sequence[Capability], action: str | None = None
    ) -> ToolkitClient:
        """Verify that the client has correct credentials and required access rights

        Args:
            capabilities (Capability | Sequence[Capability]): access capabilities to verify
            action (str, optional): What you are trying to do. It is used with the error message Defaults to None.

        Returns:
            ToolkitClient: Verified client with access rights
        """
        token_inspect = self._token_inspection
        missing_capabilities = self.toolkit_client.iam.compare_capabilities(token_inspect.capabilities, capabilities)
        if missing_capabilities:
            missing = "  - \n".join(repr(c) for c in missing_capabilities)
            first_sentence = "Don't have correct access rights"
            if action:
                first_sentence += f" to {action}."
            else:
                first_sentence += "."

            raise AuthorizationError(
                f"{first_sentence} Missing:\n{missing}\n"
                f"Please [blue][link={URL.auth_toolkit}]click here[/link][/blue] to visit the documentation "
                "and ensure that you have setup authentication for the CDF toolkit correctly."
            )
        return self.toolkit_client

    def verify_dataset(
        self, data_set_external_id: str, skip_validation: bool = False, action: str | None = None
    ) -> int:
        """Verify that the configured data set exists and is accessible

        Args:
            data_set_external_id (str): External_id of the data set to verify
            skip_validation (bool): Skip validation of the data set. If this is set, the function will
                not check for access rights to the data set and return -1 if the dataset does not exist
                or you don't have access rights to it. Defaults to False.
           action (str, optional): What you are trying to do. It is used with the error message Defaults to None.

        Returns:
            data_set_id (int)
        """
        if data_set_external_id in self._cache.data_set_id_by_external_id:
            return self._cache.data_set_id_by_external_id[data_set_external_id]
        if skip_validation:
            return -1

        self.verify_authorization(
            DataSetsAcl(
                [DataSetsAcl.Action.Read],
                ExtractionPipelinesAcl.Scope.All(),
            ),
            action=action,
        )

        try:
            data_set = self.toolkit_client.data_sets.retrieve(external_id=data_set_external_id)
        except CogniteAPIError as e:
            raise ResourceRetrievalError(f"Failed to retrieve data set {data_set_external_id}: {e}")

        if data_set is not None and data_set.id is not None:
            self._cache.data_set_id_by_external_id[data_set_external_id] = data_set.id
            return data_set.id
        raise ToolkitResourceMissingError(
            f"Data set {data_set_external_id} does not exist, you need to create it first. "
            f"Do this by adding a config file to the data_sets folder.",
            data_set_external_id,
        )

    def verify_extraction_pipeline(
        self, external_id: str, skip_validation: bool = False, action: str | None = None
    ) -> int:
        """Verify that the configured extraction pipeline exists and is accessible

        Args:
            external_id (str): External id of the extraction pipeline to verify
            skip_validation (bool): Skip validation of the extraction pipeline. If this is set, the function will
                not check for access rights to the extraction pipeline and return -1 if the extraction pipeline does not exist
                or you don't have access rights to it. Defaults to False.
            action (str, optional): What you are trying to do. It is used with the error message Defaults to None.

        Yields:
            extraction pipeline id (int)
        """
        if external_id in self._cache.extraction_pipeline_id_by_external_id:
            return self._cache.extraction_pipeline_id_by_external_id[external_id]
        if skip_validation:
            return -1

        self.verify_authorization(
            ExtractionPipelinesAcl([ExtractionPipelinesAcl.Action.Read], ExtractionPipelinesAcl.Scope.All()), action
        )
        try:
            pipeline = self.toolkit_client.extraction_pipelines.retrieve(external_id=external_id)
        except CogniteAPIError as e:
            raise ResourceRetrievalError(f"Failed to retrieve extraction pipeline {external_id}: {e}")

        if pipeline is not None and pipeline.id is not None:
            self._cache.extraction_pipeline_id_by_external_id[external_id] = pipeline.id
            return pipeline.id

        raise ToolkitResourceMissingError(
            "Extraction pipeline does not exist. You need to create it first.", external_id
        )

    @overload
    def verify_security_categories(
        self, names: str, skip_validation: bool = False, action: str | None = None
    ) -> int: ...

    @overload
    def verify_security_categories(
        self, names: list[str], skip_validation: bool = False, action: str | None = None
    ) -> list[int]: ...

    def verify_security_categories(
        self, names: str | list[str], skip_validation: bool = False, action: str | None = None
    ) -> int | list[int]:
        if skip_validation:
            return [-1 for _ in range(len(names))] if isinstance(names, list) else -1
        if isinstance(names, str) and names in self._cache.security_categories_by_name:
            return self._cache.security_categories_by_name[names]
        elif isinstance(names, list):
            existing_by_name: dict[str, int] = {
                name: self._cache.security_categories_by_name[name]
                for name in names
                if name in self._cache.security_categories_by_name
            }
            if len(existing_by_name) == len(names):
                return [existing_by_name[name] for name in names]

        self.verify_authorization(
            SecurityCategoriesAcl([SecurityCategoriesAcl.Action.List], SecurityCategoriesAcl.Scope.All()), action
        )

        all_security_categories = self.toolkit_client.iam.security_categories.list(limit=-1)
        self._cache.security_categories_by_name.update(
            {sc.name: sc.id for sc in all_security_categories if sc.id and sc.name}
        )

        try:
            if isinstance(names, str):
                return self._cache.security_categories_by_name[names]
            return [self._cache.security_categories_by_name[name] for name in names]
        except KeyError as e:
            raise ToolkitResourceMissingError(
                f"Security category {e} does not exist. You need to create it first.", e.args[0]
            ) from e

    @overload
    def verify_asset(self, external_id: str, skip_validation: bool = False, action: str | None = None) -> int: ...

    @overload
    def verify_asset(
        self, external_id: list[str], skip_validation: bool = False, action: str | None = None
    ) -> list[int]: ...

    def verify_asset(
        self, external_id: str | list[str], skip_validation: bool = False, action: str | None = None
    ) -> int | list[int]:
        if skip_validation:
            return [-1 for _ in range(len(external_id))] if isinstance(external_id, list) else -1

        if isinstance(external_id, str) and external_id in self._cache.asset_id_by_external_id:
            return self._cache.asset_id_by_external_id[external_id]
        elif isinstance(external_id, str):
            missing_external_ids = [external_id]
        elif isinstance(external_id, list):
            existing_by_external_id: dict[str, int] = {
                ext_id: self._cache.asset_id_by_external_id[ext_id]
                for ext_id in external_id
                if ext_id in self._cache.asset_id_by_external_id
            }
            if len(existing_by_external_id) == len(existing_by_external_id):
                return [existing_by_external_id[ext_id] for ext_id in external_id]
            missing_external_ids = [ext_id for ext_id in external_id if ext_id not in existing_by_external_id]
        else:
            raise ValueError(f"Expected external_id to be str or list of str, but got {type(external_id)}")

        self.verify_authorization(AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.All()), action)

        missing_assets = self.toolkit_client.assets.retrieve_multiple(
            external_ids=missing_external_ids, ignore_unknown_ids=True
        )

        self._cache.asset_id_by_external_id.update(
            {asset.external_id: asset.id for asset in missing_assets if asset.id and asset.external_id}
        )

        if missing := [ext_id for ext_id in missing_external_ids if ext_id not in self._cache.asset_id_by_external_id]:
            raise ToolkitResourceMissingError(
                "Asset(s) does not exist. You need to create it/them first.", str(missing)
            )

        if isinstance(external_id, str):
            return self._cache.asset_id_by_external_id[external_id]
        else:
            return [self._cache.asset_id_by_external_id[ext_id] for ext_id in external_id]

    @overload
    def verify_timeseries(self, external_id: str, skip_validation: bool = False, action: str | None = None) -> int: ...

    @overload
    def verify_timeseries(
        self, external_id: list[str], skip_validation: bool = False, action: str | None = None
    ) -> list[int]: ...

    def verify_timeseries(
        self, external_id: str | list[str], skip_validation: bool = False, action: str | None = None
    ) -> int | list[int]:
        if skip_validation:
            return [-1 for _ in range(len(external_id))] if isinstance(external_id, list) else -1

        if isinstance(external_id, str) and external_id in self._cache.timeseries_id_by_external_id:
            return self._cache.timeseries_id_by_external_id[external_id]
        elif isinstance(external_id, str):
            missing_external_ids = [external_id]
        elif isinstance(external_id, list):
            existing_by_external_id: dict[str, int] = {
                ext_id: self._cache.timeseries_id_by_external_id[ext_id]
                for ext_id in external_id
                if ext_id in self._cache.timeseries_id_by_external_id
            }
            if len(existing_by_external_id) == len(existing_by_external_id):
                return [existing_by_external_id[ext_id] for ext_id in external_id]
            missing_external_ids = [ext_id for ext_id in external_id if ext_id not in existing_by_external_id]
        else:
            raise ValueError(f"Expected external_id to be str or list of str, but got {type(external_id)}")

        self.verify_authorization(TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All()), action)

        missing_timeseries = self.toolkit_client.time_series.retrieve_multiple(
            external_ids=missing_external_ids, ignore_unknown_ids=True
        )

        self._cache.timeseries_id_by_external_id.update(
            {
                timeseries.external_id: timeseries.id
                for timeseries in missing_timeseries
                if timeseries.id and timeseries.external_id
            }
        )

        if missing := [
            ext_id for ext_id in missing_external_ids if ext_id not in self._cache.timeseries_id_by_external_id
        ]:
            raise ToolkitResourceMissingError(
                "TimeSeries(s) does not exist. You need to create it/them first.", str(missing)
            )

        if isinstance(external_id, str):
            return self._cache.timeseries_id_by_external_id[external_id]
        else:
            return [self._cache.timeseries_id_by_external_id[ext_id] for ext_id in external_id]

    @overload
    def verify_locationfilter(
        self, external_id: str, skip_validation: bool = False, action: str | None = None
    ) -> int: ...

    @overload
    def verify_locationfilter(
        self, external_id: list[str], skip_validation: bool = False, action: str | None = None
    ) -> list[int]: ...

    def verify_locationfilter(
        self, external_id: str | list[str], skip_validation: bool = False, action: str | None = None
    ) -> int | list[int]:
        if skip_validation:
            return [-1 for _ in range(len(external_id))] if isinstance(external_id, list) else -1

        if isinstance(external_id, str) and external_id in self._cache.locationfilter_id_by_external_id:
            return self._cache.locationfilter_id_by_external_id[external_id]
        elif isinstance(external_id, str):
            missing_external_ids = [external_id]
        elif isinstance(external_id, list):
            existing_by_external_id: dict[str, int] = {
                ext_id: self._cache.locationfilter_id_by_external_id[ext_id]
                for ext_id in external_id
                if ext_id in self._cache.locationfilter_id_by_external_id
            }
            if len(existing_by_external_id) == len(existing_by_external_id):
                return [existing_by_external_id[ext_id] for ext_id in external_id]
            missing_external_ids = [ext_id for ext_id in external_id if ext_id not in existing_by_external_id]
        else:
            raise ValueError(f"Expected external_id to be str or list of str, but got {type(external_id)}")

        self.verify_authorization(
            LocationFiltersAcl([LocationFiltersAcl.Action.Read], LocationFiltersAcl.Scope.All()), action
        )

        # Location filter cannot retrieve by external_id, so need to lookup all.
        all_filters = self.toolkit_client.location_filters.list()

        self._cache.locationfilter_id_by_external_id.update(
            {
                locationfilter.external_id: locationfilter.id
                for locationfilter in all_filters
                if locationfilter.id and locationfilter.external_id
            }
        )

        if missing := [
            ext_id for ext_id in missing_external_ids if ext_id not in self._cache.locationfilter_id_by_external_id
        ]:
            raise ToolkitResourceMissingError(
                "LocationFilter(s) does not exist. You need to create it/them first.", str(missing)
            )

        if isinstance(external_id, str):
            return self._cache.locationfilter_id_by_external_id[external_id]
        else:
            return [self._cache.locationfilter_id_by_external_id[ext_id] for ext_id in external_id]

    def create_client(self, credentials: ClientCredentials) -> ToolkitClient:
        if self._auth_vars.token_url is None or self._auth_vars.scopes is None:
            raise AuthenticationError("Token URL and Scopes are required to create a client.")
        if self._project is None or self._cdf_url is None:
            raise AuthenticationError("Project and CDF URL are required to create a client.")
        service_credentials = OAuthClientCredentials(
            token_url=self._auth_vars.token_url,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            scopes=[self._auth_vars.scopes],
        )

        return ToolkitClient(
            config=ClientConfig(
                client_name=self._client_name,
                project=self._project,
                base_url=self._cdf_url,
                credentials=service_credentials,
            )
        )
