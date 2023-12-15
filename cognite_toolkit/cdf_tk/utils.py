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
import inspect
import itertools
import json
import logging
import os
import re
import typing
from collections import UserList
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import Any, ClassVar, Generic, TypeVar, get_origin

import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.config import global_config
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError
from cognite.client.utils._text import to_camel_case, to_snake_case
from rich import print

from cognite_toolkit._version import __version__
from cognite_toolkit.cdf_tk._get_type_hints import _TypeHints

logger = logging.getLogger(__name__)


class CDFToolConfig:
    """Configurations for how to store data in CDF

    Properties:
        client: active CogniteClient
    Functions:
        verify_client: verify that the client has correct credentials and specified access capabilities
        verify_dataset: verify that the data set exists and that the client has access to it

    """

    def __init__(self, token: str | None = None, cluster: str | None = None, project: str | None = None) -> None:
        self._data_set_id: int = 0
        self._data_set = None
        self._failed = False
        self._environ = {}
        self._data_set_id_by_external_id: dict[str, id] = {}
        self._existing_spaces: set[str] = set()
        self.oauth_credentials = OAuthClientCredentials(
            token_url="",
            client_id="",
            client_secret="",
            scopes=[],
        )
        # ClientName is used for logging usage of the CDF-Toolkit.
        client_name = f"CDF-Toolkit:{__version__}"

        # CDF_CLUSTER and CDF_PROJECT are minimum requirements and can be overridden
        # when instansiating the class.
        if cluster is not None and len(cluster) > 0:
            self._cluster = cluster
            self._environ["CDF_CLUSTER"] = cluster
        if project is not None and len(project) > 0:
            self._project = project
            self._environ["CDF_PROJECT"] = project
        if token is not None:
            self._environ["CDF_TOKEN"] = token
        if (
            self.environ("CDF_URL", default=None, fail=False) is None
            and self.environ("CDF_CLUSTER", default=None, fail=False) is None
        ):
            # If CDF_URL and CDF_CLUSTER are not set, we may be in a Jupyter notebook in Fusion,
            # and credentials are preset to logged in user (no env vars are set!).
            try:
                self._client = CogniteClient()
            except Exception:
                print(
                    "[bold yellow]WARNING[/] Not able to successfully configure a Cognite client. Requirements: CDF_CLUSTER and CDF_PROJECT environment variables or CDF_TOKEN to a valid OAuth2 token."
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
        if self.environ("CDF_TOKEN", default=None, fail=False) is not None or token is not None:
            self._client = CogniteClient(
                ClientConfig(
                    client_name=client_name,
                    base_url=self._cdf_url,
                    project=self._project,
                    credentials=Token(token or self.environ("CDF_TOKEN")),
                )
            )
        else:
            # We are now doing OAuth2 client credentials flow, so we need to set the
            # required variables.
            # We can infer scopes and audience from the cluster value.
            # However, the URL to use to retrieve the token, as well as
            # the client id and secret, must be set as environment variables.
            self._scopes = [
                self.environ(
                    "IDP_SCOPES",
                    f"https://{self._cluster}.cognitedata.com/.default",
                )
            ]
            self._audience = self.environ("IDP_AUDIENCE", f"https://{self._cluster}.cognitedata.com")
            self.oauth_credentials = OAuthClientCredentials(
                token_url=self.environ("IDP_TOKEN_URL"),
                client_id=self.environ("IDP_CLIENT_ID"),
                # client secret should not be stored in-code, so we load it from an environment variable
                client_secret=self.environ("IDP_CLIENT_SECRET"),
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

    def environment_variables(self) -> dict[str, str]:
        return self._environ.copy()

    def as_string(self):
        environment = self._environ.copy()
        if "IDP_CLIENT_SECRET" in environment:
            environment["IDP_CLIENT_SECRET"] = "***"
        if "TRANSFORMATIONS_CLIENT_SECRET" in environment:
            environment["TRANSFORMATIONS_CLIENT_SECRET"] = "***"
        envs = ""
        for e in environment:
            envs += f"  {e}={environment[e]}\n"
        return f"Cluster {self._cluster} with project {self._project} and config:\n{envs}"

    def __str__(self):
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
    def failed(self, value: bool):
        self._failed = value

    @property
    def client(self) -> CogniteClient:
        return self._client

    @property
    def project(self) -> str:
        return self._project

    @property
    def data_set_id(self) -> int | None:
        return self._data_set_id if self._data_set_id > 0 else None

    # Use this to ignore the data set when verifying the client's access capabilities
    def clear_dataset(self):
        self._data_set_id = 0
        self._data_set = None

    def environ(self, attr: str, default: str | list[str] | None = None, fail: bool = True) -> str:
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
        if attr in self._environ and self._environ[attr] is not None:
            return self._environ[attr]
        # If the var was none, we want to re-evaluate from environment.
        self._environ[attr] = os.environ.get(attr, None)
        if self._environ[attr] is None:
            if default is None and fail:
                raise ValueError(f"{attr} property is not available as an environment variable and no default set.")
            self._environ[attr] = default
        return self._environ[attr]

    @property
    def data_set(self) -> str:
        return self._data_set

    @data_set.setter
    def data_set(self, value: str):
        if value is None:
            raise ValueError("Please provide an externalId of a dataset.")
        self._data_set = value
        # Since we now have a new configuration, check the dataset and set the id
        self._data_set_id = self.verify_dataset(data_set_external_id=value)

    def verify_client(
        self,
        capabilities: list[dict[str, list[str]]] | None = None,
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
        scope = {}
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
            ] or None
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

    def verify_dataset(self, data_set_external_id: str) -> int:
        """Verify that the configured data set exists and is accessible

        Args:
            data_set_external_id (str): External_id of the data set to verify
        Returns:
            data_set_id (int)
            Re-raises underlying SDK exception
        """
        if data_set_external_id in self._data_set_id_by_external_id:
            return self._data_set_id_by_external_id[data_set_external_id]

        self.verify_client(capabilities={"datasetsAcl": ["READ"]})
        try:
            data_set = self.client.data_sets.retrieve(external_id=data_set_external_id)
        except CogniteAPIError as e:
            raise CogniteAuthError("Don't have correct access rights. Need READ and WRITE on datasetsAcl.") from e
        if data_set is not None:
            self._data_set_id_by_external_id[data_set_external_id] = data_set.id
            return data_set.id
        raise ValueError(
            f"Data set {data_set_external_id} does not exist, you need to create it first. Do this by adding a config file to the data_sets folder."
        )

    def verify_extraction_pipeline(self, external_id: str) -> int:
        """Verify that the configured extraction pipeline exists and is accessible

        Args:
            external_id (str): External id of the extraction pipeline to verify
        Yields:
            extraction pipeline id (int)
            Re-raises underlying SDK exception
        """

        self.verify_client(capabilities={"extractionPipelinesAcl": ["READ"]})
        try:
            pipeline = self.client.extraction_pipelines.retrieve(external_id=external_id)
        except CogniteAPIError as e:
            raise CogniteAuthError("Don't have correct access rights. Need READ on extractionPipelinesAcl.") from e

        if pipeline is not None:
            return pipeline.id
        else:
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
            space = [space]
        if all([s in self._existing_spaces for s in space]):
            return space

        self.verify_client(capabilities={"dataModelsAcl": ["READ"]})
        try:
            existing = self.client.data_modeling.spaces.retrieve(space)
        except CogniteAPIError as e:
            raise CogniteAuthError("Don't have correct access rights. Need READ on dataModelsAcl.") from e

        if missing := (({space} if isinstance(space, str) else set(space)) - set(existing.as_ids())):
            raise ValueError(
                f"Space {missing} does not exist, you need to create it first. Do this by adding a config file to the data model folder."
            )
        self._existing_spaces.update([space.space for space in existing])
        return [space.space for space in existing]


def load_yaml_inject_variables(filepath: Path, variables: dict[str, str]) -> dict[str, Any] | list[dict[str, Any]]:
    content = filepath.read_text()
    for key, value in variables.items():
        if value is None:
            continue
        content = content.replace("${%s}" % key, value)
    return yaml.safe_load(content)


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

    def __lt__(self, other: SnakeCaseWarning) -> bool:
        if not isinstance(other, SnakeCaseWarning):
            return NotImplemented
        return (self.filepath, self.id_value, self.expected, self.actual) < (
            other.filepath,
            other.id_value,
            other.expected,
            other.actual,
        )

    def __eq__(self, other: SnakeCaseWarning) -> bool:
        if not isinstance(other, SnakeCaseWarning):
            return NotImplemented
        return (self.filepath, self.id_value, self.expected, self.actual) == (
            other.filepath,
            other.id_value,
            other.expected,
            other.actual,
        )

    def __str__(self):
        return f"CaseWarning: Got {self.actual!r}. Did you mean {self.expected!r}?"


@total_ordering
@dataclass(frozen=True)
class TemplateVariableWarning(LoadWarning):
    path: str

    def __lt__(self, other: TemplateVariableWarning) -> bool:
        if not isinstance(other, TemplateVariableWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.path) < (other.id_name, other.id_value, other.path)

    def __eq__(self, other: TemplateVariableWarning) -> bool:
        if not isinstance(other, TemplateVariableWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.path) == (other.id_name, other.id_value, other.path)

    def __str__(self):
        return f"{type(self).__name__}: Variable {self.id_name!r} has value {self.id_value!r} in file: {self.filepath.name}. Did you forget to change it?"


@total_ordering
@dataclass(frozen=True)
class DataSetMissingWarning(LoadWarning):
    resource_name: str

    def __lt__(self, other: DataSetMissingWarning) -> bool:
        if not isinstance(other, DataSetMissingWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.filepath) < (other.id_name, other.id_value, other.filepath)

    def __eq__(self, other: DataSetMissingWarning) -> bool:
        if not isinstance(other, DataSetMissingWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.filepath) == (other.id_name, other.id_value, other.filepath)

    def __str__(self):
        # Avoid circular import
        from cognite_toolkit.cdf_tk.load import TransformationLoader

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
    def __str__(self):
        output = [""]
        for path, module_warnings in itertools.groupby(sorted(self), key=lambda w: w.path):
            if path:
                output.append(f"    In Section {str(path)!r}")
            for warning in module_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class DataSetMissingWarningList(Warnings[DataSetMissingWarning]):
    def __str__(self):
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
            if issubclass(type_hint, CogniteObject):
                warnings.extend(_validate_case_raw(value, type_hint, filepath, identifier_key, identifier_value))
                continue

            container_type = get_origin(type_hint)
            if container_type not in [dict, dict, collections.abc.MutableMapping, collections.abc.Mapping]:
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


def validate_config_yaml(config: dict[str, Any], filepath: Path, path: str = "") -> TemplateVariableWarningList:
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
            warnings.extend(validate_config_yaml(value, filepath, f"{path}{key}"))
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
