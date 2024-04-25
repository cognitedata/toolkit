from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal, overload

from cognite.client import ClientConfig, CogniteClient
from cognite.client.config import global_config
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes.capabilities import Capability, FunctionsAcl
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError
from dotenv import load_dotenv


class CDFClientTool:
    """Configurations for how to store data in CDF

    Properties:
        client: active CogniteClient
    Functions:
        verify_client: verify that the client has correct credentials and specified access capabilities
        verify_dataset: verify that the data set exists and that the client has access to it

    """

    def __init__(self, client: CogniteClient() | None = None, env_path: str | None = None) -> None:
        self._environ: dict[str, str | None] = {}
        if client is None:
            self.init_local_client(env_path)
        else:
            self._client = client
        global_config.disable_pypi_version_check = True

    def init_local_client(self, env_path: str | None = None) -> CogniteClient:
        if env_path is not None:
            if not (dotenv_file := Path(env_path)).is_file():
                print(f"{env_path} does not exist.")
                exit(1)
            if dotenv_file.is_file():
                try:
                    path_str = dotenv_file.relative_to(Path.cwd())
                except ValueError:
                    path_str = dotenv_file.absolute()
                print(f"Loading .env file: {path_str!s}")
                load_dotenv(dotenv_file, override=True)

        self.oauth_credentials = OAuthClientCredentials(
            token_url="",
            client_id="",
            client_secret="",
            scopes=[],
        )
        # ClientName is used for logging usage of the CDF-Toolkit.
        client_name = "CDF-Toolkit:local-function-runner"

        if (
            self.environ("CDF_URL", default=None, fail=False) is None
            and self.environ("CDF_CLUSTER", default=None, fail=False) is None
        ):
            print(
                "ERROR Not able to successfully configure a Cognite client. Requirements: CDF_CLUSTER and CDF_PROJECT environment variables."
            )
            return

        # CDF_CLUSTER and CDF_PROJECT are minimum requirements to know where to connect.
        # Above they were forced default to None and fail was False, here we
        # will fail with an exception if they are not set.
        self._cluster = self.environ("CDF_CLUSTER")
        self._project = self.environ("CDF_PROJECT")
        # CDF_URL is optional, but if set, we use that instead of the default URL using cluster.
        self._cdf_url = self.environ("CDF_URL", f"https://{self._cluster}.cognitedata.com")
        # If CDF_TOKEN is set, we want to use that token instead of client credentials.
        if self.environ("CDF_TOKEN", default=None, fail=False) is not None:
            self._client = CogniteClient(
                ClientConfig(
                    client_name=client_name,
                    base_url=self._cdf_url,
                    project=self._project,
                    credentials=Token(self.environ("CDF_TOKEN")),
                )
            )
        else:
            # We are now doing OAuth2 client credentials flow, so we need to set the
            # required variables.
            # We can infer scopes and audience from the cluster value.
            # However, the URL to use to retrieve the token, as well as
            # the client id and secret, must be set as environment variables.
            self._scopes: list[str] = [
                self.environ(
                    "IDP_SCOPES",
                    f"https://{self._cluster}.cognitedata.com/.default",
                )
            ]
            self._audience = self.environ("IDP_AUDIENCE", f"https://{self._cluster}.cognitedata.com")
            self.oauth_credentials = OAuthClientCredentials(
                token_url=self.environ("IDP_TOKEN_URL"),
                client_id=self.environ("FUNCTION_CLIENT_ID", fail=False) or self.environ("IDP_CLIENT_ID"),
                # client secret should not be stored in-code, so we load it from an environment variable
                client_secret=self.environ("FUNCTION_CLIENT_SECRET", fail=False) or self.environ("IDP_CLIENT_SECRET"),
                scopes=self._scopes,
                audience=self._audience,
            )
            global_config.disable_pypi_version_check = True
            self._client = CogniteClient(
                ClientConfig(
                    client_name=client_name,
                    base_url=self._cdf_url,
                    project=self._project,
                    credentials=self.oauth_credentials,
                )
            )

    def environment_variables(self) -> dict[str, str | None]:
        return {**self._environ.copy(), **os.environ}

    def as_string(self) -> str:
        environment = os.environ.copy()
        if "IDP_CLIENT_SECRET" in environment:
            environment["IDP_CLIENT_SECRET"] = "***"
        if "TRANSFORMATIONS_CLIENT_SECRET" in environment:
            environment["TRANSFORMATIONS_CLIENT_SECRET"] = "***"
        if "FUNCTIONS_CLIENT_SECRET" in environment:
            environment["FUNCTIONS_CLIENT_SECRET"] = "***"
        envs = ""
        for e in environment:
            envs += f"  {e}={environment[e]}\n"
        return f"CDF URL {self._client.config.base_url} with project {self._client.config.project} and config:\n{envs}"

    def __str__(self) -> str:
        return self.as_string()

    @property
    def client(self) -> CogniteClient:
        return self._client

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
        return self._client

    def verify_capabilities(self, capability: Capability | Sequence[Capability]) -> CogniteClient:
        missing_capabilities = self._client.iam.verify_capabilities(capability)
        if len(missing_capabilities) > 0:
            raise CogniteAuthError(f"Missing capabilities: {missing_capabilities}")
        return self._client

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
        if data_set_external_id in self._data_set_id_by_external_id:
            return self._data_set_id_by_external_id[data_set_external_id]

        if not skip_validation:
            self.verify_client(capabilities={"datasetsAcl": ["READ"]})
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
            self._data_set_id_by_external_id[data_set_external_id] = data_set.id
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

        if all([s in self._existing_spaces for s in spaces]):
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
        self._existing_spaces.update([space.space for space in existing])
        return [space.space for space in existing]


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    tool = CDFClientTool(client=client)
    # This will fail unless the function has the specified capabilities.
    tool.verify_capabilities(
        [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
        ]
    )
    print(tool)
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
