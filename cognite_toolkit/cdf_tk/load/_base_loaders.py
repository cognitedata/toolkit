from __future__ import annotations

import inspect
import re
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Collection, Sequence, Sized
from pathlib import Path
from typing import Any, Generic, TypeVar, Union

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.capabilities import Capability
from cognite.client.data_classes.data_modeling import DataModelingId, VersionedDataModelingId
from cognite.client.data_classes.data_modeling.ids import InstanceId
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit.cdf_tk.load._data_classes import RawTable
from cognite_toolkit.cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

T_ID = TypeVar("T_ID", bound=Union[str, int, DataModelingId, InstanceId, VersionedDataModelingId, RawTable])
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)


class Loader(ABC):
    """This is the base class for all loaders"""

    filetypes: frozenset[str]
    dependencies: frozenset[type[ResourceLoader]] = frozenset()

    def __init__(self, client: CogniteClient):
        self.client = client

    @classmethod
    @abstractmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")


class ResourceLoader(
    Loader,
    ABC,
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
):
    """This is the base class for all resource loaders.

    A resource loader is a standardized interface for loading resources from YAML files and uploading them to CDF.

    It consists of the four data classes and the CRUD methods that are used to interact with the CDF API.

    All resources supported by the cognite_toolkit should implement a loader.

    Class attributes:
        support_drop: Whether the resource supports the drop flag.
        filetypes: The filetypes that are supported by this loader. If empty, all files are supported.
        api_name: The name of the api that is in the cognite_client that is used to interact with the CDF API.
        folder_name: The name of the folder in the build directory where the files are located.
        resource_cls: The class of the resource that is loaded.
        list_cls: The read list format for this resource.
        dependencies: A set of loaders that must be loaded before this loader.
        _display_name: The name of the resource that is used when printing messages. If this is not set the
            api_name is used.
    """

    support_drop = True
    filetypes = frozenset({"yaml", "yml"})
    filename_pattern = ""
    api_name: str
    folder_name: str
    resource_write_cls: type[T_WriteClass]
    resource_cls: type[T_WritableCogniteResource]
    list_cls: type[T_WritableCogniteResourceList]
    list_write_cls: type[T_CogniteResourceList]
    identifier_key: str = "externalId"
    dependencies: frozenset[type[ResourceLoader]] = frozenset()
    _display_name: str = ""

    def __init__(self, client: CogniteClient, ToolGlobals: CDFToolConfig):
        super().__init__(client)
        self.ToolGlobals = ToolGlobals
        try:
            self.api_class = self._get_api_class(client, self.api_name)
        except AttributeError:
            raise AttributeError(f"Invalid api_name {self.api_name}.")

    @property
    def display_name(self) -> str:
        if self._display_name:
            return self._display_name
        return self.api_name

    @staticmethod
    def _get_api_class(client: CogniteClient, api_name: str) -> Any:
        parent = client
        if (dot_count := Counter(api_name)["."]) == 1:
            parent_name, api_name = api_name.split(".")
            parent = getattr(client, parent_name)
        elif dot_count == 0:
            pass
        else:
            raise AttributeError(f"Invalid api_name {api_name}.")
        return getattr(parent, api_name)

    @classmethod
    def create_loader(
        cls, ToolGlobals: CDFToolConfig
    ) -> ResourceLoader[
        T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
    ]:
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return cls(client, ToolGlobals)

    @classmethod
    @abstractmethod
    def get_id(cls, item: T_WriteClass | T_WritableCogniteResource) -> T_ID:
        raise NotImplementedError

    @classmethod
    def get_ids(cls, items: Sequence[T_WriteClass | T_WritableCogniteResource]) -> list[T_ID]:
        return [cls.get_id(item) for item in items]

    @classmethod
    def find_files(cls, dir_or_file: Path) -> list[Path]:
        """Find all files that are supported by this loader in the given directory or file.

        Args:
            dir_or_file (Path): The directory or file to search in.

        Returns:
            list[Path]: A list of all files that are supported by this loader.

        """
        if dir_or_file.is_file():
            if dir_or_file.suffix not in cls.filetypes or not cls.filetypes:
                raise ValueError("Invalid file type")
            return [dir_or_file]
        elif dir_or_file.is_dir():
            if cls.filetypes:
                file_paths = (file for type_ in cls.filetypes for file in dir_or_file.glob(f"**/*.{type_}"))
            else:
                file_paths = dir_or_file.glob("**/*")

            if cls.filename_pattern:
                pattern = re.compile(cls.filename_pattern)
                return [file for file in file_paths if pattern.match(file.stem)]
            else:
                return list(file_paths)
        else:
            return []

    # Default implementations that can be overridden
    def load_resource(self, filepath: Path, skip_validation: bool) -> T_WriteClass | T_CogniteResourceList:
        raw_yaml = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_write_cls.load(raw_yaml)
        return self.resource_write_cls.load(raw_yaml)

    def create(self, items: T_CogniteResourceList, drop: bool, filepath: Path) -> Sized:
        try:
            created = self.api_class.create(items)
            return created
        except CogniteAPIError as e:
            if e.code == 409:
                print("  [bold yellow]WARNING:[/] Resource(s) already exist(s), skipping creation.")
                return self.list_cls([])
            else:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                self.ToolGlobals.failed = True
                return self.list_cls([])
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} out of {len(items)} resource(s) already exist(s). {len(e.successful or [])} resource(s) created."
            )
            return self.list_cls([])
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return self.list_cls([])

    def retrieve(self, ids: SequenceNotStr[T_ID]) -> T_WritableCogniteResourceList:
        if hasattr(self.api_class, "retrieve_multiple"):
            if inspect.signature(self.api_class.retrieve_multiple).parameters.get("ignore_unknown_ids"):
                retrieved = self.api_class.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)
            else:
                retrieved = self.api_class.retrieve_multiple(external_ids=ids)
        else:
            retrieved = self.api_class.retrieve(ids)
        if retrieved is None:
            return self.list_cls([])
        elif isinstance(retrieved, self.list_cls):
            return retrieved
        elif isinstance(retrieved, Collection):
            return self.list_cls(retrieved)
        else:
            return self.list_cls([retrieved])

    def update(self, items: T_CogniteResourceList, filepath: Path) -> Sized:
        return self.api_class.update(items)

    def delete(self, ids: SequenceNotStr[T_ID], drop_data: bool) -> int:
        self.api_class.delete(ids)
        return len(ids)


class DataLoader(Loader, ABC):
    @abstractmethod
    def upload(self, datafile: Path) -> bool:
        raise NotImplementedError
