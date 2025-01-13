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
from dataclasses import _MISSING_TYPE, dataclass, field, fields
from pathlib import Path
from typing import Any, Literal, TypeAlias, overload

import questionary
import typer
from cognite.client.config import global_config
from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCredentials,
    OAuthDeviceCode,
    OAuthInteractive,
    Token,
)
from cognite.client.data_classes import ClientCredentials
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich import print
from rich.prompt import Prompt

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    TOOLKIT_CLIENT_ENTRA_ID,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import IgnoredValueWarning, MediumSeverityWarning
from cognite_toolkit._version import __version__

LoginFlow: TypeAlias = Literal["client_credentials", "token", "device_code", "interactive"]
Provider: TypeAlias = Literal["entra_id", "cdf", "other"]

CLIENT_NAME = f"CDF-Toolkit:{__version__}"
LOGIN_FLOW_DESCRIPTION = {
    "client_credentials": "Setup a service principal with client credentials",
    "interactive": "Login using the browser with your user credentials",
    "device_code": "Login using the browser with your user credentials using device code flow",
    "token": "Use a Token directly to authenticate",
}

PROVDER_DESCRIPTION = {
    "entra_id": "Use Microsoft Entra ID to authenticate",
    "cdf": "Use Cognite IDP to authenticate",
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
    cdf_client_timeout: int = field(
        default=30,
        metadata=dict(
            env_name="CDF_CLIENT_TIMEOUT",
            display_name="CDF client timeout",
            example="30",
        ),
    )
    cdf_client_max_workers: int = field(
        default=5,
        metadata=dict(
            env_name="CDF_CLIENT_MAX_WORKERS",
            display_name="CDF client max workers",
            example="5",
        ),
    )

    def __post_init__(self) -> None:
        # Set defaults based on cluster and tenant_id
        if self.cluster:
            self.set_cluster_defaults()
        if self.provider == "cdf":
            self.set_cdf_provider_defaults()
        if self.tenant_id and self.provider != "cdf":
            self.set_token_id_defaults()
        if self.token and self.login_flow != "token":
            print(
                f"  [bold yellow]Warning[/] CDF_TOKEN detected. This will override LOGIN_FLOW, "
                f"thus LOGIN_FLOW={self.login_flow} will be ignored"
            )
            self.login_flow = "token"

        if isinstance(self.cdf_client_timeout, str):
            try:
                self.cdf_client_timeout = int(self.cdf_client_timeout)
            except ValueError:
                raise ToolkitValueError(f"CDF_CLIENT_TIMEOUT must be an integer, got {self.cdf_client_timeout}")
        if isinstance(self.cdf_client_max_workers, str):
            try:
                self.cdf_client_max_workers = int(self.cdf_client_max_workers)
            except ValueError:
                raise ToolkitValueError(f"CDF_CLIENT_MAX_WORKERS must be an integer, got {self.cdf_client_max_workers}")

    def set_cdf_provider_defaults(self, force: bool = False) -> None:
        default_url = "https://auth.cognite.com/oauth2/token"
        if force:
            self.token_url = default_url
        else:
            self.token_url = self.token_url or default_url
        if self.scopes is not None:
            IgnoredValueWarning("IDP_SCOPES", self.scopes, "Provider Cog-IDP does not need scopes").print_warning()
        self.scopes = None

    def set_token_id_defaults(self) -> None:
        if self.tenant_id:
            self.token_url = self.token_url or f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            self.authority_url = self.authority_url or f"https://login.microsoftonline.com/{self.tenant_id}"

    def set_cluster_defaults(self) -> None:
        if self.cluster:
            self.cdf_url = self.cdf_url or f"https://{self.cluster}.cognitedata.com"
            self.audience = self.audience or f"https://{self.cluster}.cognitedata.com"
            if self.provider != "cdf":
                self.scopes = self.scopes or f"https://{self.cluster}.cognitedata.com/.default"

    @property
    def is_complete(self) -> bool:
        if self.cdf_url is None:
            return False
        if self.login_flow == "token":
            return self.token is not None
        elif self.login_flow == "interactive":
            return self.client_id is not None and self.tenant_id is not None and self.scopes is not None
        elif self.login_flow == "client_credentials" and self.provider == "cdf":
            return self.client_id is not None and self.client_secret is not None
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
                default = None if isinstance(field_.default, _MISSING_TYPE) else field_.default
                args[field_.name] = override.get(env_name, os.environ.get(env_name, default))
        return cls(**args)

    def create_dotenv_file(self) -> str:
        lines = [
            "# .env file generated by cognite-toolkit",
            self._write_var("login_flow"),
            self._write_var("cluster"),
            self._write_var("project"),
        ]
        if self.login_flow != "token":
            lines += [
                self._write_var("provider"),
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
        elif self.login_flow == "device_code" and self.provider != "entra_id":
            ...
        elif self.login_flow == "interactive":
            lines += [
                self._write_var("client_id"),
            ]
        else:
            raise ValueError(f"Login flow {self.login_flow} is not supported.")
        if self.login_flow in ("client_credentials", "interactive") and self.provider != "cdf":
            lines += [
                "# Note: Either the TENANT_ID or the TENANT_URL must be written.",
                self._write_var("tenant_id"),
                self._write_var("token_url"),
            ]
        elif self.login_flow == "client_credentials" and self.provider == "cdf":
            lines += [
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
        if self.login_flow in ("client_credentials", "interactive") and self.provider != "cdf":
            lines += [
                self._write_var("scopes"),
            ]
        if self.login_flow == "interactive":
            lines += [
                self._write_var("authority_url"),
            ]
        if (self.login_flow == "client_credentials" and self.provider != "cdf") or (
            self.login_flow == "device_code" and self.provider == "other"
        ):
            lines += [
                self._write_var("audience"),
            ]
        lines += [
            "# The below variables control the client configuration.",
            self._write_var("cdf_client_timeout"),
            self._write_var("cdf_client_max_workers"),
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
        provider = questionary.select(
            "Choose the provider",
            choices=[
                Choice(title=f"{provider}: {description}", value=provider)
                for provider, description in PROVDER_DESCRIPTION.items()
            ],
        ).ask()
        auth_vars.provider = provider

        if login_flow == "client_credentials" and auth_vars.provider == "cdf":
            auth_vars.scopes = None
            auth_vars.set_cdf_provider_defaults(force=True)
        elif login_flow in ("interactive", "device_code", "client_credentials") and auth_vars.provider == "entra_id":
            auth_vars.tenant_id = self.prompt_user("tenant_id")
            auth_vars.set_token_id_defaults()
        elif login_flow == "device_code" and auth_vars.provider == "other":
            auth_vars.client_id = self.prompt_user("client_id")
            auth_vars.oidc_discovery_url = self.prompt_user("oidc_discovery_url")

        default_variables = ["cdf_url"]
        if login_flow == "client_credentials" and provider != "cdf":
            default_variables.extend(["scopes", "audience"])
        elif login_flow == "client_credentials" and provider == "cdf":
            default_variables.extend(["token_url"])
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
            existing = Path(".env").read_text(encoding="utf-8")
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
                Path(".env").write_text(new_env_file, encoding="utf-8")
        elif questionary.confirm("Do you want to save these to .env file for next time?", default=True).ask():
            Path(".env").write_text(new_env_file, encoding="utf-8")

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

    """

    def __init__(
        self,
        token: str | None = None,
        cluster: str | None = None,
        project: str | None = None,
        cdf_url: str | None = None,
        auth_vars: AuthVariables | None = None,
        skip_initialization: bool = False,
    ) -> None:
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
        self._client_name = CLIENT_NAME

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
        if _RUNNING_IN_BROWSER and auth_vars is None:
            self._initialize_in_browser()
            return

        self._auth_vars = auth_vars or AuthVariables.from_env(self._environ)
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
        elif auth.login_flow == "client_credentials" and auth.provider == "cdf":
            if not (auth.client_id and auth.client_secret):
                raise AuthenticationError(
                    "Login flow=client_credentials is set but missing required authentication "
                    "variables: IDP_CLIENT_ID and IDP_CLIENT_SECRET. Cannot authenticate the client."
                )
            self._credentials_args = dict(
                token_url=auth.token_url,
                client_id=auth.client_id,
                client_secret=auth.client_secret,
                scopes=None,
            )
            self._credentials_provider = OAuthClientCredentials(**self._credentials_args)
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
            ToolkitClientConfig(
                client_name=self._client_name,
                base_url=self._cdf_url,
                project=self._project,
                credentials=self._credentials_provider,
                timeout=auth.cdf_client_timeout,
            )
        )
        global_config.max_workers = auth.cdf_client_max_workers
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

    @property
    def cdf_cluster(self) -> str:
        if self._cluster is None:
            raise ValueError("Cluster is not initialized.")
        return self._cluster

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
            config=ToolkitClientConfig(
                client_name=self._client_name,
                project=self._project,
                base_url=self._cdf_url,
                credentials=service_credentials,
            )
        )
