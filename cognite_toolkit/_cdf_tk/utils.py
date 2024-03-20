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

import abc
import collections
import hashlib
import inspect
import itertools
import json
import logging
import os
import re
import sys
import types
import typing
from abc import abstractmethod
from collections import UserDict, UserList, defaultdict
from collections.abc import Collection, ItemsView, KeysView, Sequence, ValuesView
from dataclasses import dataclass, field, fields
from functools import total_ordering
from pathlib import Path
from typing import Any, ClassVar, Generic, Literal, TypeVar, Union, cast, get_args, get_origin, overload

import typer
import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.config import global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive, Token
from cognite.client.data_classes import CreatedSession
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.capabilities import Capability
from cognite.client.data_classes.data_modeling import View, ViewId
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError
from cognite.client.testing import CogniteClientMock
from cognite.client.utils._text import to_camel_case, to_snake_case
from rich import print
from rich.prompt import Confirm, Prompt

from cognite_toolkit._cdf_tk._get_type_hints import _TypeHints
from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._version import __version__

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

logger = logging.getLogger(__name__)


LoginFlow: TypeAlias = Literal["client_credentials", "token", "interactive"]


@dataclass
class AuthVariables:
    cluster: str | None = field(
        metadata=dict(env_name="CDF_CLUSTER", display_name="CDF project cluster", example="westeurope-1")
    )
    project: str | None = field(
        metadata=dict(env_name="CDF_PROJECT", display_name="CDF project URL name", example="publicdata")
    )
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
        default=None, metadata=dict(env_name="IDP_CLIENT_ID", display_name="client id", example="")
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
            self.cdf_url = self.cdf_url or f"https://{self.cluster}.cognitedata.com"
            self.audience = self.audience or f"https://{self.cluster}.cognitedata.com"
            self.scopes = self.scopes or f"https://{self.cluster}.cognitedata.com/.default"
        if self.tenant_id:
            self.token_url = self.token_url or f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            self.authority_url = self.authority_url or f"https://login.microsoftonline.com/{self.tenant_id}"

    @classmethod
    def login_flow_options(cls) -> list[str]:
        return list(get_args(LoginFlow))

    @classmethod
    def from_env(cls, override: dict[str, Any] | None = None) -> AuthVariables:
        override = override or {}
        args: dict[str, Any] = {}
        for field_ in fields(cls):
            if env_name := field_.metadata.get("env_name"):
                args[field_.name] = override.get(env_name, os.environ.get(env_name))
        return cls(**args)

    def validate(self, verbose: bool) -> AuthReaderValidation:
        return self._read_and_validate(verbose, skip_prompt=True)

    def from_interactive_with_validation(self, verbose: bool = False) -> AuthReaderValidation:
        return self._read_and_validate(verbose, skip_prompt=False)

    def _read_and_validate(self, verbose: bool = False, skip_prompt: bool = False) -> AuthReaderValidation:
        reader = AuthReaderValidation(self, verbose, skip_prompt)
        self.cluster = reader.prompt_user("cluster")
        self.project = reader.prompt_user("project")
        if not (self.cluster or self.project):
            reader.status = "error"
            reader.messages.append("  [bold red]ERROR[/]: CDF Cluster and project are required.")
            return reader
        self.cdf_url = reader.prompt_user("cdf_url", expected=f"https://{self.cluster}.cognitedata.com")
        self.login_flow = reader.prompt_user("login_flow", choices=self.login_flow_options())  # type: ignore[assignment]
        if self.login_flow == "token":
            if new_token := reader.prompt_user("token", password=True):
                self.token = new_token
            else:
                print("  Keeping existing token.")
        elif self.login_flow in ("client_credentials", "interactive"):
            self.audience = reader.prompt_user("audience", expected=f"https://{self.cluster}.cognitedata.com")
            self.scopes = reader.prompt_user("scopes")
            self.tenant_id = reader.prompt_user("tenant_id")
            self.token_url = reader.prompt_user("token_url")
            self.client_id = reader.prompt_user("client_id")
            if self.login_flow == "client_credentials":
                if new_secret := reader.prompt_user("client_secret", password=True):
                    self.client_secret = new_secret
                else:
                    print("  Keeping existing client secret.")
        else:
            reader.status = "error"
            reader.messages.append(f"The login flow {self.login_flow} is not supported")

        if not skip_prompt:
            if Path(".env").exists():
                print(
                    "[bold yellow]WARNING[/]: .env file already exists and values have been retrieved from it. It will be overwritten."
                )
            write = Confirm.ask(
                "Do you want to save these to .env file for next time ? ",
                choices=["y", "n"],
            )
            if write:
                self.write_dotenv_file()

        return reader

    def write_dotenv_file(self) -> None:
        lines = [
            "# .env file generated by cognite-toolkit",
            self._write_var("cluster"),
            self._write_var("project"),
            self._write_var("login_flow"),
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
        lines += [
            "# The below variables don't have to be set if you have just accepted the defaults.",
            "# They are automatically constructed unless they are set.",
            self._write_var("cdf_url"),
        ]
        if self.login_flow == "client_credentials":
            lines += [
                "# Note: Either the TENANT_ID or the TENANT_URL must be written.",
                self._write_var("tenant_id"),
                self._write_var("token_url"),
                self._write_var("audience"),
                self._write_var("scopes"),
            ]

        Path(".env").write_text("\n".join(lines))
        return None

    def _write_var(self, var_name: str) -> str:
        value = getattr(self, var_name)
        field_ = _auth_field_by_name[var_name].metadata
        return f"{field_['env_name']}={value}"


class AuthReaderValidation:
    """Reads and validate the auth variables

    Args:
        auth_vars (AuthVariables): The auth variables to validate
        verbose (bool): If True, print additional information
        skip_prompt (bool): If True, skip prompting the user for input
            and only do the validation.

    """

    def __init__(self, auth_vars: AuthVariables, verbose: bool, skip_prompt: bool = False):
        self._auth_vars = auth_vars
        self.status: Literal["ok", "error", "warning"] = "ok"
        self.messages: list[str] = []
        self.verbose = verbose
        self.skip_prompt = skip_prompt

    def prompt_user(
        self,
        field_name: str,
        choices: list[str] | None = None,
        password: bool | None = None,
        expected: str | None = None,
    ) -> str | None:
        try:
            current_value = getattr(self._auth_vars, field_name)
            field_ = _auth_field_by_name[field_name]
            metadata = field_.metadata
            example = (
                metadata["example"]
                .replace("CDF_CLUSTER", self._auth_vars.cluster or "<cluster>")
                .replace("IDP_TENANT_ID", self._auth_vars.tenant_id or "<tenant_id>")
            )
            display_name = metadata["display_name"]
            default = cast(Union[str, None], current_value or field_.default)
        except Exception as e:
            raise RuntimeError("AuthVariables not created correctly. Contact Support") from e

        extra_args: dict[str, Any] = {}
        if password is False:
            extra_args["default"] = ""
        else:
            extra_args["default"] = default
        if choices:
            extra_args["choices"] = choices
        if password is not None:
            extra_args["password"] = password

        if password and current_value:
            prompt = f"You have set {display_name}, change it? (press Enter to keep current value)"
        elif example == default:
            prompt = f"{display_name}? "
        else:
            prompt = f"{display_name} (e.g. [italic]{example}[/])? "
        response: str | None
        if self.skip_prompt:
            response = default
        else:
            response = Prompt.ask(prompt, **extra_args)
        if not expected or response == expected:
            if self.verbose:
                self.messages.append(f"  {display_name}={response} is set correctly.")
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
        client: active CogniteClient
    Functions:
        verify_client: verify that the client has correct credentials and specified access capabilities
        verify_dataset: verify that the data set exists and that the client has access to it

    """

    @dataclass
    class _Cache:
        existing_spaces: set[str] = field(default_factory=set)
        data_set_id_by_external_id: dict[str, int] = field(default_factory=dict)

    def __init__(self, token: str | None = None, cluster: str | None = None, project: str | None = None) -> None:
        self._cache = self._Cache()
        self._failed = False
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
        self._client: CogniteClient | None = None

        global_config.disable_pypi_version_check = True
        if _RUNNING_IN_BROWSER:
            self._initialize_in_browser()
            return

        auth_vars = AuthVariables.from_env(self._environ)
        self.initialize_from_auth_variables(auth_vars)

    def _initialize_in_browser(self) -> None:
        try:
            self._client = CogniteClient()
        except Exception as e:
            print(f"[bold red]Error[/] Failed to initialize CogniteClient in browser: {e}")
        else:
            if self._cluster or self._project:
                print("[bold yellow]Warning[/] Cluster and project are arguments ignored when running in the browser.")
            self._cluster = self._client.config.base_url.removeprefix("https://").split(".", maxsplit=1)[0]
            self._project = self._client.config.project
            self._cdf_url = self._client.config.base_url

    def initialize_from_auth_variables(self, auth: AuthVariables) -> bool:
        """Initialize the CDFToolConfig from the AuthVariables and returns whether it was successful or not."""
        cluster = auth.cluster or self._cluster
        project = auth.project or self._project

        if cluster is None or project is None:
            print("  [bold red]Error[/] Cluster and Project must be set to authenticate the client.")
            return False

        self._cluster = cluster
        self._project = project
        self._cdf_url = auth.cdf_url or self._cdf_url

        credentials_provider: CredentialProvider
        if auth.token or auth.login_flow == "token":
            if auth.login_flow != "token":
                print(
                    f"  [bold yellow]Warning[/] CDF_TOKEN detected. This will override LOGIN_FLOW, "
                    f"thus LOGIN_FLOW={auth.login_flow} will be ignored"
                )
            if not auth.token:
                print("  [bold red]Error[/] Login flow=token is set but no token is provided.")
                return False
            credentials_provider = Token(auth.token)
        elif auth.login_flow == "interactive":
            if auth.scopes:
                self._scopes = [auth.scopes]
            if not (auth.client_id and auth.authority_url and auth.scopes):
                print(
                    "  [bold red]Error[/] Login flow=interactive is set but missing required authentication "
                    "variables. Cannot initialize Cognite client."
                )
                return False
            credentials_provider = OAuthInteractive(
                authority_url=auth.authority_url,
                client_id=auth.client_id,
                scopes=self._scopes,
            )
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
                print(
                    "  [bold yellow]Error[/] Login flow=client_credentials is set but missing required authentication variables. Cannot initialize cognite client."
                )
                return False

            credentials_provider = OAuthClientCredentials(
                token_url=auth.token_url,
                client_id=auth.client_id,
                client_secret=auth.client_secret,
                scopes=self._scopes,
                audience=self._audience,
            )
        else:
            print(f"  [bold red]Error[/] Login flow {auth.login_flow} is not supported.")
            return False

        self._client = CogniteClient(
            ClientConfig(
                client_name=self._client_name,
                base_url=self._cdf_url,
                project=self._project,
                credentials=credentials_provider,
            )
        )
        self._update_environment_variables()
        return True

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
            self._environ[env_name] = value[0] if isinstance(value, list) else value

    @classmethod
    def from_context(cls, ctx: typer.Context) -> CDFToolConfig:
        if ctx.obj.mockToolGlobals is not None:
            return ctx.obj.mockToolGlobals
        else:
            return CDFToolConfig(cluster=ctx.obj.cluster, project=ctx.obj.project)

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
    # Flag set if something that should have worked failed if a data set is
    # loaded and/or deleted.
    def failed(self) -> bool:
        return self._failed

    @failed.setter
    def failed(self, value: bool) -> None:
        self._failed = value

    @property
    def client(self) -> CogniteClient:
        if self._client is None:
            raise ValueError("Client is not initialized.")
        return self._client

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

    def verify_client(
        self,
        capabilities: dict[str, list[str]] | None = None,
        data_set_id: int = 0,
        space_id: str | None = None,
    ) -> CogniteClient:
        """Verify that the client has correct credentials and required access rights

        Supply requirement CDF ACLs to verify if you have correct access
        capabilities = {
            "filesAcl": ["READ", "WRITE"],
            "datasetsAcl": ["READ", "WRITE"]
        }
        The data_set_id will be used when verifying that the client has access to the dataset.
        This approach can be reused for any usage of the Cognite Python SDK.

        Args:
            capabilities (dict[list], optional): access capabilities to verify
            data_set_id (int): id of dataset that access should be granted to
            space_id (str): id of space that access should be granted to

        Yields:
            CogniteClient: Verified client with access rights
            Re-raises underlying SDK exception
        """
        capabilities = capabilities or {}
        try:
            # Using the token/inspect endpoint to check if the client has access to the project.
            # The response also includes access rights, which can be used to check if the client has the
            # correct access for what you want to do.
            resp = self.client.iam.token.inspect()
            if resp is None or len(resp.capabilities.data) == 0:
                raise CogniteAuthError("Don't have any access rights. Check credentials.")
        except Exception as e:
            raise e
        scope: dict[str, dict[str, Any]] = {}
        if data_set_id > 0:
            scope["dataSetScope"] = {"ids": [data_set_id]}
        if space_id is not None:
            scope["spaceScope"] = {"ids": [space_id]}
        if space_id is None and data_set_id == 0:
            scope["all"] = {}
        try:
            caps = [
                Capability.load(
                    {
                        cap: {
                            "actions": actions,
                            "scope": scope,
                        },
                    }
                )
                for cap, actions in capabilities.items()
            ]
        except Exception:
            raise ValueError(f"Failed to load capabilities from {capabilities}. Wrong syntax?")
        comp = self.client.iam.compare_capabilities(resp.capabilities, caps)
        if len(comp) > 0:
            print(f"Missing necessary CDF access capabilities: {comp}")
            raise CogniteAuthError("Don't have correct access rights.")
        return self.client

    def verify_capabilities(self, capability: Capability | Sequence[Capability]) -> CogniteClient:
        missing_capabilities = self.client.iam.verify_capabilities(capability)
        if len(missing_capabilities) > 0:
            raise CogniteAuthError(f"Missing capabilities: {missing_capabilities}")
        return self.client

    def verify_dataset(self, data_set_external_id: str, skip_validation: bool = False) -> int:
        """Verify that the configured data set exists and is accessible

        Args:
            data_set_external_id (str): External_id of the data set to verify
            skip_validation (bool): Skip validation of the data set. If this is set, the function will
                not check for access rights to the data set and return -1 if the dataset does not exist
                or you don't have access rights to it. Defaults to False.
        Returns:
            data_set_id (int)
            Re-raises underlying SDK exception
        """
        if data_set_external_id in self._cache.data_set_id_by_external_id:
            return self._cache.data_set_id_by_external_id[data_set_external_id]

        try:
            data_set = self.client.data_sets.retrieve(external_id=data_set_external_id)
        except CogniteAPIError as e:
            if skip_validation:
                return -1
            raise CogniteAuthError("Don't have correct access rights. Need READ and WRITE on datasetsAcl.") from e
        except Exception as e:
            if skip_validation:
                return -1
            raise e
        if data_set is not None and data_set.id is not None:
            self._cache.data_set_id_by_external_id[data_set_external_id] = data_set.id
            return data_set.id
        if skip_validation:
            return -1
        raise ValueError(
            f"Data set {data_set_external_id} does not exist, you need to create it first. Do this by adding a config file to the data_sets folder."
        )

    def verify_extraction_pipeline(self, external_id: str, skip_validation: bool = False) -> int:
        """Verify that the configured extraction pipeline exists and is accessible

        Args:
            external_id (str): External id of the extraction pipeline to verify
            skip_validation (bool): Skip validation of the extraction pipeline. If this is set, the function will
                not check for access rights to the extraction pipeline and return -1 if the extraction pipeline does not exist
                or you don't have access rights to it. Defaults to False.
        Yields:
            extraction pipeline id (int)
            Re-raises underlying SDK exception
        """
        if not skip_validation:
            self.verify_client(capabilities={"extractionPipelinesAcl": ["READ"]})
        try:
            pipeline = self.client.extraction_pipelines.retrieve(external_id=external_id)
        except CogniteAPIError as e:
            if skip_validation:
                return -1
            raise CogniteAuthError("Don't have correct access rights. Need READ on extractionPipelinesAcl.") from e
        except Exception as e:
            if skip_validation:
                return -1
            raise e

        if pipeline is not None and pipeline.id is not None:
            return pipeline.id

        if not skip_validation:
            print(
                f"  [bold yellow]WARNING[/] Extraction pipeline {external_id} does not exist. It may have been deleted, or not been part of the module."
            )
        return -1

    def verify_spaces(self, space: str | list[str]) -> list[str]:
        """Verify that the configured space exists and is accessible

        Args:
            space (str): External id of the space to verify

        Yields:
            spaces (str)
            Re-raises underlying SDK exception
        """
        if isinstance(space, str):
            spaces = [space]
        else:
            spaces = space

        if all([s in self._cache.existing_spaces for s in spaces]):
            return spaces

        self.verify_client(capabilities={"dataModelsAcl": ["READ"]})
        try:
            existing = self.client.data_modeling.spaces.retrieve(spaces)
        except CogniteAPIError as e:
            raise CogniteAuthError("Don't have correct access rights. Need READ on dataModelsAcl.") from e

        if missing := (set(spaces) - set(existing.as_ids())):
            raise ValueError(
                f"Space {missing} does not exist, you need to create it first. Do this by adding a config file to the data model folder."
            )
        self._cache.existing_spaces.update([space.space for space in existing])
        return [space.space for space in existing]


@overload
def load_yaml_inject_variables(
    filepath: Path, variables: dict[str, str | None], required_return_type: Literal["list"]
) -> list[dict[str, Any]]: ...


@overload
def load_yaml_inject_variables(
    filepath: Path, variables: dict[str, str | None], required_return_type: Literal["dict"]
) -> dict[str, Any]: ...


@overload
def load_yaml_inject_variables(
    filepath: Path, variables: dict[str, str | None], required_return_type: Literal["any"] = "any"
) -> dict[str, Any] | list[dict[str, Any]]: ...


def load_yaml_inject_variables(
    filepath: Path, variables: dict[str, str | None], required_return_type: Literal["any", "list", "dict"] = "any"
) -> dict[str, Any] | list[dict[str, Any]]:
    content = filepath.read_text()
    for key, value in variables.items():
        if value is None:
            continue
        content = content.replace("${%s}" % key, value)
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
        config_data = yaml.safe_load(filepath.read_text())
    except yaml.YAMLError as e:
        print(f"  [bold red]ERROR:[/] reading {filepath}: {e}")
        return {}
    if expected_output == "list" and isinstance(config_data, dict):
        print(f"  [bold red]ERROR:[/] {filepath} is not a list")
        exit(1)
    elif expected_output == "dict" and isinstance(config_data, list):
        print(f"  [bold red]ERROR:[/] {filepath} is not a dict")
        exit(1)
    return config_data


@dataclass(frozen=True)
class LoadWarning:
    _type: ClassVar[str]
    filepath: Path
    id_value: str
    id_name: str


@total_ordering
@dataclass(frozen=True)
class SnakeCaseWarning(LoadWarning):
    actual: str
    expected: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SnakeCaseWarning):
            return NotImplemented
        return (self.filepath, self.id_value, self.expected, self.actual) < (
            other.filepath,
            other.id_value,
            other.expected,
            other.actual,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SnakeCaseWarning):
            return NotImplemented
        return (self.filepath, self.id_value, self.expected, self.actual) == (
            other.filepath,
            other.id_value,
            other.expected,
            other.actual,
        )

    def __str__(self) -> str:
        return f"CaseWarning: Got {self.actual!r}. Did you mean {self.expected!r}?"


@total_ordering
@dataclass(frozen=True)
class TemplateVariableWarning(LoadWarning):
    path: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TemplateVariableWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.path) < (other.id_name, other.id_value, other.path)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TemplateVariableWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.path) == (other.id_name, other.id_value, other.path)

    def __str__(self) -> str:
        return f"{type(self).__name__}: Variable {self.id_name!r} has value {self.id_value!r} in file: {self.filepath.name}. Did you forget to change it?"


@total_ordering
@dataclass(frozen=True)
class DataSetMissingWarning(LoadWarning):
    resource_name: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, DataSetMissingWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.filepath) < (other.id_name, other.id_value, other.filepath)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataSetMissingWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.filepath) == (other.id_name, other.id_value, other.filepath)

    def __str__(self) -> str:
        # Avoid circular import
        from cognite_toolkit._cdf_tk.load import TransformationLoader

        if self.filepath.parent.name == TransformationLoader.folder_name:
            return f"{type(self).__name__}: It is recommended to use a data set if source or destination can be scoped with a data set. If not, ignore this warning."
        else:
            return f"{type(self).__name__}: It is recommended that you set dataSetExternalId for {self.resource_name}. This is missing in {self.filepath.name}. Did you forget to add it?"


T_Warning = TypeVar("T_Warning", bound=LoadWarning)


class Warnings(UserList, Generic[T_Warning]):
    def __init__(self, collection: Collection[T_Warning] | None = None):
        super().__init__(collection or [])


class SnakeCaseWarningList(Warnings[SnakeCaseWarning]):
    def __str__(self) -> str:
        output = [""]
        for (file, identifier, id_name), file_warnings in itertools.groupby(
            sorted(self), key=lambda w: (w.filepath, w.id_value, w.id_name)
        ):
            output.append(f"    In File {str(file)!r}")
            output.append(f"    In entry {id_name}={identifier!r}")
            for warning in file_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class TemplateVariableWarningList(Warnings[TemplateVariableWarning]):
    def __str__(self) -> str:
        output = [""]
        for path, module_warnings in itertools.groupby(sorted(self), key=lambda w: w.path):
            if path:
                output.append(f"    In Section {str(path)!r}")
            for warning in module_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class DataSetMissingWarningList(Warnings[DataSetMissingWarning]):
    def __str__(self) -> str:
        output = [""]
        for filepath, warnings in itertools.groupby(sorted(self), key=lambda w: w.filepath):
            output.append(f"    In file {str(filepath)!r}")
            for warning in warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


def validate_case_raw(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
) -> SnakeCaseWarningList:
    """Checks whether camel casing the raw data would match a parameter in the resource class.

    Args:
        raw: The raw data to check.
        resource_cls: The resource class to check against init method
        filepath: The filepath of the raw data. This is used to pass to the warnings for easy
            grouping of warnings.
        identifier_key: The key to use as identifier. Defaults to "externalId". This is used to pass to the warnings
            for easy grouping of warnings.

    Returns:
        A list of CaseWarning objects.

    """
    return _validate_case_raw(raw, resource_cls, filepath, identifier_key)


def _validate_case_raw(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
    identifier_value: str = "",
) -> SnakeCaseWarningList:
    warnings = SnakeCaseWarningList()
    if isinstance(raw, list):
        for item in raw:
            warnings.extend(_validate_case_raw(item, resource_cls, filepath, identifier_key))
        return warnings
    elif not isinstance(raw, dict):
        return warnings

    signature = inspect.signature(resource_cls.__init__)

    is_base_class = inspect.isclass(resource_cls) and any(base is abc.ABC for base in resource_cls.__bases__)
    if is_base_class:
        # If it is a base class, it cannot be instantiated, so it can be any of the
        # subclasses' parameters.
        expected = {
            to_camel_case(parameter)
            for sub in resource_cls.__subclasses__()
            for parameter in inspect.signature(sub.__init__).parameters.keys()
        } - {"self"}
    else:
        expected = set(map(to_camel_case, signature.parameters.keys())) - {"self"}

    actual = set(raw.keys())
    actual_camel_case = set(map(to_camel_case, actual))
    snake_cased = actual - actual_camel_case

    if not identifier_value:
        identifier_value = raw.get(
            identifier_key, raw.get(to_snake_case(identifier_key), f"No identifier {identifier_key}")
        )

    for key in snake_cased:
        if (camel_key := to_camel_case(key)) in expected:
            warnings.append(SnakeCaseWarning(filepath, identifier_value, identifier_key, str(key), str(camel_key)))

    try:
        type_hints_by_name = _TypeHints.get_type_hints_by_name(signature, resource_cls)
    except Exception:
        # If we cannot get type hints, we cannot check if the type is correct.
        return warnings

    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        if (parameter := signature.parameters.get(to_snake_case(key))) and (
            type_hint := type_hints_by_name.get(parameter.name)
        ):
            if inspect.isclass(type_hint) and issubclass(type_hint, CogniteObject):
                warnings.extend(_validate_case_raw(value, type_hint, filepath, identifier_key, identifier_value))
                continue

            container_type = get_origin(type_hint)
            if sys.version_info >= (3, 10):
                # UnionType was introduced in Python 3.10
                if container_type is types.UnionType:
                    args = typing.get_args(type_hint)
                    type_hint = next((arg for arg in args if arg is not type(None)), None)

            mappings = [dict, collections.abc.MutableMapping, collections.abc.Mapping]
            is_mapping = container_type in mappings or (
                isinstance(type_hint, types.GenericAlias) and len(typing.get_args(type_hint)) == 2
            )
            if not is_mapping:
                continue
            args = typing.get_args(type_hint)
            if not args:
                continue
            container_key, container_value = args
            if inspect.isclass(container_value) and issubclass(container_value, CogniteObject):
                for sub_key, sub_value in value.items():
                    warnings.extend(
                        _validate_case_raw(sub_value, container_value, filepath, identifier_key, identifier_value)
                    )

    return warnings


def validate_modules_variables(config: dict[str, Any], filepath: Path, path: str = "") -> TemplateVariableWarningList:
    """Checks whether the config file has any issues.

    Currently, this checks for:
        * Non-replaced template variables, such as <change_me>.

    Args:
        config: The config to check.
        filepath: The filepath of the config.yaml.
        path: The path in the config.yaml. This is used recursively by this function.
    """
    warnings = TemplateVariableWarningList()
    pattern = re.compile(r"<.*?>")
    for key, value in config.items():
        if isinstance(value, str) and pattern.match(value):
            warnings.append(TemplateVariableWarning(filepath, value, key, path))
        elif isinstance(value, dict):
            if path:
                path += "."
            warnings.extend(validate_modules_variables(value, filepath, f"{path}{key}"))
    return warnings


def validate_data_set_is_set(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
) -> DataSetMissingWarningList:
    warnings = DataSetMissingWarningList()
    signature = inspect.signature(resource_cls.__init__)
    if "data_set_id" not in set(signature.parameters.keys()):
        return warnings

    if isinstance(raw, list):
        for item in raw:
            warnings.extend(validate_data_set_is_set(item, resource_cls, filepath, identifier_key))
        return warnings

    if "dataSetExternalId" in raw or "dataSetId" in raw:
        return warnings

    value = raw.get(identifier_key, raw.get(to_snake_case(identifier_key), f"No identifier {identifier_key}"))
    warnings.append(DataSetMissingWarning(filepath, value, identifier_key, resource_cls.__name__))
    return warnings


def resolve_relative_path(path: Path, base_path: Path | str) -> Path:
    """
    This is useful if we provide a relative path to some resource in a config file.
    """
    if path.is_absolute():
        raise ValueError(f"Path {path} is not relative.")

    if isinstance(base_path, str):
        base_path = Path(base_path)

    if not base_path.is_dir():
        base_path = base_path.parent

    return (base_path / path).resolve()


def calculate_directory_hash(directory: Path, exclude_prefixes: set[str] | None = None) -> str:
    sha256_hash = hashlib.sha256()

    # Walk through each file in the directory
    for filepath in sorted(directory.rglob("*"), key=lambda p: str(p.relative_to(directory))):
        if filepath.is_dir():
            continue
        if exclude_prefixes and any(filepath.name.startswith(prefix) for prefix in exclude_prefixes):
            continue
        relative_path = filepath.relative_to(directory)
        sha256_hash.update(relative_path.as_posix().encode("utf-8"))
        # Open each file and update the hash
        with filepath.open("rb") as file:
            while chunk := file.read(8192):
                # Get rid of Windows line endings to make the hash consistent across platforms.
                sha256_hash.update(chunk.replace(b"\r\n", b"\n"))

    return sha256_hash.hexdigest()


def get_oneshot_session(client: CogniteClient) -> CreatedSession | None:
    """Get a oneshot (use once) session for execution in CDF"""
    # Special case as this utility function may be called with a new client created in code,
    # it's hard to mock it in tests.
    if isinstance(client, CogniteClientMock):
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


def retrieve_view_ancestors(client: CogniteClient, parents: list[ViewId], cache: dict[ViewId, View]) -> list[View]:
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
