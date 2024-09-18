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

import difflib
import hashlib
import itertools
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import typing
from abc import abstractmethod
from collections import UserDict, defaultdict
from collections.abc import ItemsView, Iterable, Iterator, KeysView, MutableSequence, Sequence, ValuesView
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeVar, overload

import questionary
import typer
import yaml
from cognite.client import ClientConfig
from cognite.client.config import global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive, Token
from cognite.client.data_classes import CreatedSession
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataSetsAcl,
    ExtractionPipelinesAcl,
    SecurityCategoriesAcl,
)
from cognite.client.data_classes.data_modeling import DataModelId, View, ViewId
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich import print
from rich.prompt import Prompt

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER, BUILTIN_MODULES, ROOT_MODULES, URL
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ResourceRetrievalError,
    ToolkitError,
    ToolkitResourceMissingError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._version import __version__

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias


if TYPE_CHECKING:
    from sentry_sdk.types import Event as SentryEvent
    from sentry_sdk.types import Hint as SentryHint


logger = logging.getLogger(__name__)


LoginFlow: TypeAlias = Literal["client_credentials", "token", "interactive"]


LOGIN_FLOW_DESCRIPTION = {
    "client_credentials": "Setup a service principal with client credentials",
    "interactive": "Login using the browser with your user credentials",
    "token": "Use a Token directly to authenticate",
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
            display_name="tenant id",
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
        self.token_url = self.token_url or f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        self.authority_url = self.authority_url or f"https://login.microsoftonline.com/{self.tenant_id}"

    def set_cluster_defaults(self) -> None:
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
        if self.login_flow == "client_credentials":
            lines += [
                self._write_var("audience"),
            ]

        return "\n".join(lines)

    def _write_var(self, var_name: str) -> str:
        value = getattr(self, var_name)
        field_ = _auth_field_by_name[var_name].metadata
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
        if auth_vars.is_complete:
            print("Auth variables are already set.")
            if not questionary.confirm("Do you want to reconfigure the auth variables?", default=False).ask():
                return auth_vars
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

        if login_flow in ("client_credentials", "interactive"):
            auth_vars.tenant_id = self.prompt_user("tenant_id")
            auth_vars.set_token_id_defaults()

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
        except Exception as e:
            raise RuntimeError("AuthVariables not created correctly. Contact Support") from e

        extra_args: dict[str, Any] = {}
        if password is True:
            extra_args["default"] = ""
        else:
            extra_args["default"] = default
        if choices:
            extra_args["choices"] = choices
        if password is not None:
            extra_args["password"] = password

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
        token_inspect: TokenInspection | None = None

    def __init__(
        self,
        token: str | None = None,
        cluster: str | None = None,
        project: str | None = None,
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

        auth_vars = AuthVariables.from_env(self._environ)
        if not skip_initialization:
            self.initialize_from_auth_variables(auth_vars)
        self._login_flow = auth_vars.login_flow

    def _initialize_in_browser(self) -> None:
        try:
            self._toolkit_client = ToolkitClient()
        except Exception as e:
            raise AuthenticationError(f"Failed to initialize CogniteClient in browser: {e}")

        if self._cluster or self._project:
            print("[bold yellow]Warning[/] Cluster and project are arguments ignored when running in the browser.")
        self._cluster = self._toolkit_client.config.base_url.removeprefix("https://").split(".", maxsplit=1)[0]
        self._project = self._toolkit_client.config.project
        self._cdf_url = self._toolkit_client.config.base_url

    def initialize_from_auth_variables(self, auth: AuthVariables) -> None:
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
            except Exception as e:
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
        return f"Cluster {self._cluster} with project {self._project} and config:\n{envs}"

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
def load_yaml_inject_variables(
    filepath: Path | str, variables: dict[str, str | None], required_return_type: Literal["list"], validate: bool = True
) -> list[dict[str, Any]]: ...


@overload
def load_yaml_inject_variables(
    filepath: Path | str, variables: dict[str, str | None], required_return_type: Literal["dict"], validate: bool = True
) -> dict[str, Any]: ...


@overload
def load_yaml_inject_variables(
    filepath: Path | str,
    variables: dict[str, str | None],
    required_return_type: Literal["any"] = "any",
    validate: bool = True,
) -> dict[str, Any] | list[dict[str, Any]]: ...


def load_yaml_inject_variables(
    filepath: Path | str,
    variables: dict[str, str | None],
    required_return_type: Literal["any", "list", "dict"] = "any",
    validate: bool = True,
) -> dict[str, Any] | list[dict[str, Any]]:
    if isinstance(filepath, str):
        content = filepath
    else:
        content = filepath.read_text()
    for key, value in variables.items():
        if value is None:
            continue
        content = content.replace(f"${{{key}}}", value)
    if validate:
        for match in re.finditer(r"\$\{([^}]+)\}", content):
            environment_variable = match.group(1)
            suffix = f" It is expected in {filepath.name}." if isinstance(filepath, Path) else ""
            MediumSeverityWarning(
                f"Variable {environment_variable} is not set in the environment.{suffix}"
            ).print_warning()

    if yaml.__with_libyaml__:
        # CSafeLoader is faster than yaml.safe_load
        result = yaml.CSafeLoader(content).get_data()
    else:
        result = yaml.safe_load(content)
    if required_return_type == "any":
        return result
    elif required_return_type == "list":
        if isinstance(result, list):
            return result
        raise ValueError(f"Expected a list, but got {type(result)}")
    elif required_return_type == "dict":
        if isinstance(result, dict):
            return result
        raise ValueError(f"Expected a dict, but got {type(result)}")
    else:
        raise ValueError(f"Unknown required_return_type {required_return_type}")


@overload
def read_yaml_file(filepath: Path, expected_output: Literal["dict"] = "dict") -> dict[str, Any]: ...


@overload
def read_yaml_file(filepath: Path, expected_output: Literal["list"]) -> list[dict[str, Any]]: ...


def read_yaml_file(
    filepath: Path, expected_output: Literal["list", "dict"] = "dict"
) -> dict[str, Any] | list[dict[str, Any]]:
    """Read a YAML file and return a dictionary

    filepath: path to the YAML file
    """
    try:
        config_data = read_yaml_content(filepath.read_text())
    except yaml.YAMLError as e:
        print(f"  [bold red]ERROR:[/] reading {filepath}: {e}")
        return {}

    if expected_output == "list" and isinstance(config_data, dict):
        ToolkitYAMLFormatError(f"{filepath} did not contain `list` as expected")
    elif expected_output == "dict" and isinstance(config_data, list):
        ToolkitYAMLFormatError(f"{filepath} did not contain `dict` as expected")
    return config_data


def read_yaml_content(content: str) -> dict[str, Any] | list[dict[str, Any]]:
    """Read a YAML string and return a dictionary

    content: string containing the YAML content
    """
    if yaml.__with_libyaml__:
        # CSafeLoader is faster than yaml.safe_load
        config_data = yaml.CSafeLoader(content).get_data()
    else:
        config_data = yaml.safe_load(content)
    return config_data


def calculate_directory_hash(
    directory: Path,
    exclude_prefixes: set[str] | None = None,
    ignore_files: set[str] | None = None,
    shorten: bool = False,
) -> str:
    sha256_hash = hashlib.sha256()

    # Walk through each file in the directory
    for filepath in sorted(directory.rglob("*"), key=lambda p: str(p.relative_to(directory))):
        if filepath.is_dir():
            continue
        if exclude_prefixes and any(filepath.name.startswith(prefix) for prefix in exclude_prefixes):
            continue
        if ignore_files and filepath.suffix in ignore_files:
            continue
        relative_path = filepath.relative_to(directory)
        sha256_hash.update(relative_path.as_posix().encode("utf-8"))
        # Open each file and update the hash
        with filepath.open("rb") as file:
            while chunk := file.read(8192):
                # Get rid of Windows line endings to make the hash consistent across platforms.
                sha256_hash.update(chunk.replace(b"\r\n", b"\n"))

    calculated = sha256_hash.hexdigest()
    if shorten:
        return calculated[:8]
    return calculated


def calculate_secure_hash(item: dict[str, Any]) -> str:
    """Calculate a secure hash of a dictionary"""
    sha256_hash = hashlib.sha512(usedforsecurity=True)
    sha256_hash.update(json.dumps(item, sort_keys=True).encode("utf-8"))
    return sha256_hash.hexdigest()


def calculate_str_or_file_hash(content: str | Path, shorten: bool = False) -> str:
    sha256_hash = hashlib.sha256()
    if isinstance(content, Path):
        content = content.read_text(encoding="utf-8")
    # Get rid of Windows line endings to make the hash consistent across platforms.
    sha256_hash.update(content.encode("utf-8").replace(b"\r\n", b"\n"))
    calculated = sha256_hash.hexdigest()
    if shorten:
        return calculated[:8]
    return calculated


def get_oneshot_session(client: ToolkitClient) -> CreatedSession | None:
    """Get a oneshot (use once) session for execution in CDF"""
    # Special case as this utility function may be called with a new client created in code,
    # it's hard to mock it in tests.
    if isinstance(client, ToolkitClientMock):
        bearer = "123"
    else:
        (_, bearer) = client.config.credentials.authorization_header()
    ret = client.post(
        url=f"/api/v1/projects/{client.config.project}/sessions",
        json={
            "items": [
                {
                    "oneshotTokenExchange": True,
                },
            ],
        },
        headers={"Authorization": bearer},
    )
    if ret.status_code == 200:
        return CreatedSession.load(ret.json()["items"][0])
    return None


@dataclass(frozen=True)
class YAMLComment:
    """This represents a comment in a YAML file. It can be either above or after a variable."""

    above: list[str] = field(default_factory=list)
    after: list[str] = field(default_factory=list)

    @property
    def comment(self) -> str:
        return "\n".join(self.above) + "\n" + "\n".join(self.after)


T_Key = TypeVar("T_Key")
T_Value = TypeVar("T_Value")


class YAMLWithComments(UserDict[T_Key, T_Value]):
    @staticmethod
    def _extract_comments(raw_file: str, key_prefix: tuple[str, ...] = tuple()) -> dict[tuple[str, ...], YAMLComment]:
        """Extract comments from a raw file and return a dictionary with the comments."""
        comments: dict[tuple[str, ...], YAMLComment] = defaultdict(YAMLComment)
        position: Literal["above", "after"]
        init_value: object = object()
        variable: str | None | object = init_value
        last_comments: list[str] = []
        last_variable: str | None = None
        last_leading_spaces = 0
        parent_variables: list[str] = []
        indent: int | None = None
        for line in raw_file.splitlines():
            if ":" in line:
                # Is variable definition
                leading_spaces = len(line) - len(line.lstrip())
                variable = str(line.split(":", maxsplit=1)[0].strip())
                if leading_spaces > last_leading_spaces and last_variable:
                    parent_variables.append(last_variable)
                    if indent is None:
                        # Automatically indent based on the first variable
                        indent = leading_spaces
                elif leading_spaces < last_leading_spaces and parent_variables:
                    parent_variables = parent_variables[: -((last_leading_spaces - leading_spaces) // (indent or 2))]

                if last_comments:
                    comments[(*key_prefix, *parent_variables, variable)].above.extend(last_comments)
                    last_comments.clear()

                last_variable = variable
                last_leading_spaces = leading_spaces

            if "#" in line:
                # Potentially has comment.
                before, comment = str(line).rsplit("#", maxsplit=1)
                position = "after" if ":" in before else "above"
                if position == "after" and (before.count('"') % 2 == 1 or before.count("'") % 2 == 1):
                    # The comment is inside a string
                    continue
                # This is a new comment.
                if (position == "after" or variable is None) and variable is not init_value:
                    key = (*key_prefix, *parent_variables, *((variable and [variable]) or []))  # type: ignore[misc]
                    if position == "after":
                        comments[key].after.append(comment.strip())
                    else:
                        comments[key].above.append(comment.strip())
                else:
                    last_comments.append(comment.strip())

        return dict(comments)

    def _dump_yaml_with_comments(self, indent_size: int = 2, newline_after_indent_reduction: bool = False) -> str:
        """Dump a config dictionary to a yaml string"""
        config = self.dump()
        dumped = yaml.dump(config, sort_keys=False, indent=indent_size)
        out_lines = []
        if comments := self._get_comment(tuple()):
            for comment in comments.above:
                out_lines.append(f"# {comment}")
        last_indent = 0
        last_variable: str | None = None
        path: tuple[str, ...] = tuple()
        for line in dumped.splitlines():
            indent = len(line) - len(line.lstrip())
            if last_indent < indent:
                if last_variable is None:
                    raise ValueError("Unexpected state of last_variable being None")
                path = (*path, last_variable)
            elif last_indent > indent:
                if newline_after_indent_reduction:
                    # Adding some extra space between modules
                    out_lines.append("")
                indent_reduction_steps = (last_indent - indent) // indent_size
                path = path[:-indent_reduction_steps]

            variable = line.split(":", maxsplit=1)[0].strip()
            if comments := self._get_comment((*path, variable)):
                for line_comment in comments.above:
                    out_lines.append(f"{' ' * indent}# {line_comment}")
                if after := comments.after:
                    line = f"{line} # {after[0]}"

            out_lines.append(line)
            last_indent = indent
            last_variable = variable
        out_lines.append("")
        return "\n".join(out_lines)

    @abstractmethod
    def dump(self) -> dict[str, Any]: ...

    @abstractmethod
    def _get_comment(self, key: tuple[str, ...]) -> YAMLComment | None: ...

    # This is to get better type hints in the IDE
    def items(self) -> ItemsView[T_Key, T_Value]:
        return super().items()

    def keys(self) -> KeysView[T_Key]:
        return super().keys()

    def values(self) -> ValuesView[T_Value]:
        return super().values()


def retrieve_view_ancestors(client: ToolkitClient, parents: list[ViewId], cache: dict[ViewId, View]) -> list[View]:
    """Retrieves all ancestors of a view.

    This will mutate the cache that is passed in, and return a list of views that are the ancestors of the views in the parents list.

    Args:
        client: The Cognite client to use for the requests
        parents: The parents of the view to retrieve all ancestors for
        cache: The cache to store the views in
    """
    parent_ids = parents
    found: list[View] = []
    while parent_ids:
        to_lookup = []
        grand_parent_ids = []
        for parent in parent_ids:
            if parent in cache:
                found.append(cache[parent])
                grand_parent_ids.extend(cache[parent].implements or [])
            else:
                to_lookup.append(parent)

        if to_lookup:
            looked_up = client.data_modeling.views.retrieve(to_lookup)
            cache.update({view.as_id(): view for view in looked_up})
            found.extend(looked_up)
            for view in looked_up:
                grand_parent_ids.extend(view.implements or [])

        parent_ids = grand_parent_ids
    return found


def sentry_exception_filter(event: SentryEvent, hint: SentryHint) -> Optional[SentryEvent]:
    if "exc_info" in hint:
        exc_type, exc_value, tb = hint["exc_info"]
        # Returning None prevents the event from being sent to Sentry
        if isinstance(exc_value, ToolkitError):
            return None
    return event


@contextmanager
def tmp_build_directory() -> typing.Generator[Path, None, None]:
    build_dir = Path(tempfile.mkdtemp(prefix="build.", suffix=".tmp", dir=Path.cwd()))
    try:
        yield build_dir
    finally:
        shutil.rmtree(build_dir)


def flatten_dict(dct: dict[str, Any]) -> dict[tuple[str, ...], Any]:
    """Flatten a dictionary to a list of tuples with the key path and value."""
    items: dict[tuple[str, ...], Any] = {}
    for key, value in dct.items():
        if isinstance(value, dict):
            for sub_key, sub_value in flatten_dict(value).items():
                items[(key, *sub_key)] = sub_value
        else:
            items[(key,)] = value
    return items


def iterate_modules(root_dir: Path) -> Iterator[tuple[Path, list[Path]]]:
    """Iterate over all modules in the project and yield the module directory and all files in the module.

    Args:
        root_dir (Path): The root directory of the project

    Yields:
        Iterator[tuple[Path, list[Path]]]: A tuple containing the module directory and a list of all files in the module

    """
    if root_dir.name in ROOT_MODULES:
        yield from _iterate_modules(root_dir)
        return
    elif root_dir.name == BUILTIN_MODULES:
        yield from _iterate_modules(root_dir)
        return
    for root_module in ROOT_MODULES:
        module_dir = root_dir / root_module
        if module_dir.exists():
            yield from _iterate_modules(module_dir)


def _iterate_modules(root_dir: Path) -> Iterator[tuple[Path, list[Path]]]:
    # local import to avoid circular import
    from .constants import EXCL_FILES
    from .loaders import LOADER_BY_FOLDER_NAME

    if not root_dir.exists():
        return
    for module_dir in root_dir.iterdir():
        if not module_dir.is_dir():
            continue
        sub_directories = [path for path in module_dir.iterdir() if path.is_dir()]
        is_any_resource_directories = any(dir.name in LOADER_BY_FOLDER_NAME for dir in sub_directories)
        if sub_directories and is_any_resource_directories:
            # Module found
            yield module_dir, [path for path in module_dir.rglob("*") if path.is_file() and path.name not in EXCL_FILES]
            # Stop searching for modules in subdirectories
            continue
        yield from _iterate_modules(module_dir)


@overload
def module_from_path(path: Path, return_resource_folder: Literal[True]) -> tuple[str, str]: ...


@overload
def module_from_path(path: Path, return_resource_folder: Literal[False] = False) -> str: ...


def module_from_path(path: Path, return_resource_folder: bool = False) -> str | tuple[str, str]:
    """Get the module name from a path"""
    # local import to avoid circular import
    from .loaders import LOADER_BY_FOLDER_NAME

    if len(path.parts) == 1:
        raise ValueError("Path is not a module")
    last_folder = path.parts[1]
    for part in path.parts[1:]:
        if part in LOADER_BY_FOLDER_NAME:
            if return_resource_folder:
                return last_folder, part
            return last_folder
        last_folder = part
    raise ValueError("Path is not part of a module")


def resource_folder_from_path(path: Path) -> str:
    """Get the resource_folder from a path"""
    # local import to avoid circular import
    from .loaders import LOADER_BY_FOLDER_NAME

    for part in path.parts:
        if part in LOADER_BY_FOLDER_NAME:
            return part
    raise ValueError("Path does not contain a resource folder")


def find_directory_with_subdirectories(
    directory_name: str | None, root_directory: Path
) -> tuple[Path | None, list[str]]:
    """Search for a directory with a specific name in the root_directory
    and return the directory and all subdirectories."""
    if directory_name is None:
        return None, []
    search = [root_directory]
    while search:
        current = search.pop()
        for root in current.iterdir():
            if not root.is_dir():
                continue
            if root.name == directory_name:
                return root, [d.name for d in root.iterdir() if d.is_dir()]
            search.append(root)
    return None, []


# Spaces are allowed, but we replace them as well
_ILLEGAL_CHARACTERS = re.compile(r"[<>:\"/\\|?*\s]")


def to_directory_compatible(text: str) -> str:
    """Convert a string to be compatible with directory names on all platforms"""
    cleaned = _ILLEGAL_CHARACTERS.sub("_", text)
    # Replace multiple underscores with a single one
    return re.sub(r"_+", "_", cleaned)


def to_diff(a: dict[str, Any], b: dict[str, Any]) -> Iterator[str]:
    a_str = yaml.safe_dump(a, sort_keys=True)
    b_str = yaml.safe_dump(b, sort_keys=True)

    return difflib.unified_diff(a_str.splitlines(), b_str.splitlines())


def safe_read(file: Path) -> str:
    """Falls back on explicit using utf-8 if the default .read_text()"""
    try:
        return file.read_text()
    except UnicodeDecodeError:
        # On Windows, we may have issues as the encoding is not always utf-8
        return file.read_text(encoding="utf-8")


def safe_write(file: Path, content: str) -> None:
    """Falls back on explicit using utf-8 if the default .write_text()"""
    try:
        file.write_text(content)
    except UnicodeEncodeError:
        # On Windows, we may have issues as the encoding is not always utf-8
        file.write_text(content, encoding="utf-8")


def in_dict(keys: Iterable[str], dictionary: dict) -> bool:
    return all(key in dictionary for key in keys)


def get_cicd_environment() -> str:
    if "CI" in os.environ and os.getenv("GITHUB_ACTIONS"):
        return "github"
    if os.getenv("GITLAB_CI"):
        return "gitlab"
    if "CI" in os.environ and "BITBUCKET_BUILD_NUMBER" in os.environ:
        return "bitbucket"
    if os.getenv("CIRCLECI"):
        return "circleci"
    if os.getenv("TRAVIS"):
        return "travis"
    if "TF_BUILD" in os.environ:
        return "azure"

    return "local"


def quote_int_value_by_key_in_yaml(content: str, key: str) -> str:
    """Quote a value in a yaml string"""
    # This pattern will match the key if it is not already quoted
    pattern = rf"^(\s*-?\s*)?{key}:\s*(?!.*['\":])([\d_]+)$"
    replacement = rf'\1{key}: "\2"'

    return re.sub(pattern, replacement, content, flags=re.MULTILINE)


def stringify_value_by_key_in_yaml(content: str, key: str) -> str:
    """Quote a value in a yaml string"""
    pattern = rf"^{key}:\s*$"
    replacement = rf"{key}: |"
    return re.sub(pattern, replacement, content, flags=re.MULTILINE)


class GraphQLParser:
    _token_pattern = re.compile(r"\w+|[^\w\s]")

    def __init__(self, raw: str, data_model_id: DataModelId) -> None:
        self.raw = raw
        self.data_model_id = data_model_id
        self._entities: list[_Entity] | None = None

    def get_views(self, include_version: bool = False) -> set[ViewId]:
        views: set[ViewId] = set()
        for entity in self._get_entities():
            if entity.is_imported:
                continue
            view_directive: _ViewDirective | None = None
            for directive in entity.directives:
                if isinstance(directive, _ViewDirective):
                    view_directive = directive
                    break
            if view_directive:
                views.add(
                    ViewId(
                        view_directive.space or self.data_model_id.space,
                        view_directive.external_id or entity.identifier,
                        version=view_directive.version if include_version else None,
                    )
                )
            else:
                views.add(ViewId(self.data_model_id.space, entity.identifier))
        return views

    def get_dependencies(self, include_version: bool = False) -> set[ViewId | DataModelId]:
        dependencies: set[ViewId | DataModelId] = set()
        for entity in self._get_entities():
            view_directive: _ViewDirective | None = None
            is_dependency = False
            for directive in entity.directives:
                if isinstance(directive, _Import):
                    if directive.data_model:
                        dependencies.add(directive.data_model)
                        break
                    is_dependency = True
                elif isinstance(directive, _ViewDirective):
                    view_directive = directive
            if is_dependency and view_directive:
                dependencies.add(
                    ViewId(
                        view_directive.space or self.data_model_id.space,
                        view_directive.external_id or entity.identifier,
                        version=view_directive.version if include_version else None,
                    )
                )
            elif is_dependency:
                # Todo: Warning Likely invalid directive
                ...
        return dependencies

    def _get_entities(self) -> list[_Entity]:
        if self._entities is None:
            self._entities = self._parse()
        return self._entities

    def _parse(self) -> list[_Entity]:
        entities: list[_Entity] = []
        entity: _Entity | None = None
        last_class: Literal["type", "interface"] | None = None

        parentheses: list[str] = []

        directive_tokens: _DirectiveTokens | None = None
        is_directive_start: bool = False
        for token in self._token_pattern.findall(self.raw):
            if token in "({[<":
                parentheses.append(token)
            elif token in ")}]>":
                parentheses.pop()

            is_end_of_entity = bool(parentheses and parentheses[0] == "{")

            if entity and is_end_of_entity:
                # End of entity definition
                if directive_tokens and (directive := directive_tokens.create()):
                    entity.directives.append(directive)
                directive_tokens = None
                entities.append(entity)
                entity = None
            elif entity is not None:
                if directive_tokens is not None and (not parentheses or token == "@"):
                    # End of directive
                    if directive := directive_tokens.create():
                        entity.directives.append(directive)
                    directive_tokens = None
                    if token == "@":
                        is_directive_start = True
                elif directive_tokens:
                    # Gather the content of the directive
                    directive_tokens.append(token)
                elif token == "@":
                    is_directive_start = True
                elif is_directive_start and token in ("import", "view"):
                    directive_tokens = _DirectiveTokens([token])
                    is_directive_start = False
                elif is_directive_start:
                    # Not a directive we care about
                    is_directive_start = False

            elif token in ("type", "interface"):
                # Next token starts a new entity definition
                last_class = token
            elif last_class is not None:
                # Start of a new entity definition
                entity = _Entity(identifier=token, class_=last_class)
                last_class = None
        return entities


class _DirectiveTokens(list, MutableSequence[str]):
    def create(self) -> _Directive | None:
        return _Directive.load(self)


@dataclass
class _Directive:
    # This pattern ignores commas inside }
    SPLIT_ON_COMMA_PATTERN = re.compile(r",(?![^{]*\})")

    @classmethod
    def load(cls, content: list[str]) -> _Directive | None:
        key, *content = content
        raw_string = "".join(content).removeprefix("(").removesuffix(")")
        data = typing.cast(dict[str, Any], cls._create_args(raw_string))
        if key == "import":
            return _Import._load(data)
        if key == "view":
            return _ViewDirective._load(data)
        return None

    @classmethod
    def _create_args(cls, string: str) -> dict[str, Any] | str:
        if "," not in string and ":" not in string:
            return string
        output: dict[str, Any] = {}
        if string[0] == "{" and string[-1] == "}":
            string = string[1:-1]
        for pair in cls.SPLIT_ON_COMMA_PATTERN.split(string):
            stripped = pair.strip()
            if not stripped or ":" not in stripped:
                continue
            key, value = cls._clean(*stripped.split(":", maxsplit=1))
            output[key] = cls._create_args(value)
        return output

    @classmethod
    def _clean(cls, *args: Any) -> Any:
        return tuple(arg.removeprefix('"').removesuffix('"').removeprefix('"').removesuffix('"') for arg in args)

    @classmethod
    @abstractmethod
    def _load(cls, data: dict[str, Any]) -> _Directive: ...


@dataclass
class _ViewDirective(_Directive):
    space: str | None = None
    external_id: str | None = None
    version: str | None = None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> _ViewDirective:
        return _ViewDirective(space=data.get("space"), external_id=data.get("externalId"), version=data.get("version"))


@dataclass
class _Import(_Directive):
    data_model: DataModelId | None = None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> _Import:
        if "dataModel" in data:
            return _Import(data_model=DataModelId.load(data["dataModel"]))
        return _Import()


@dataclass
class _Entity:
    identifier: str
    class_: Literal["type", "interface"]
    directives: list[_Directive] = field(default_factory=list)

    @property
    def is_imported(self) -> bool:
        return any(isinstance(directive, _Import) for directive in self.directives)
