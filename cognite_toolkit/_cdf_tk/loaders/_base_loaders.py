from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable, Sequence, Sized
from functools import lru_cache
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar, overload

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet, read_parameter_from_init_type_hints
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.tk_warnings import WarningList, YAMLFileWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

T_ID = TypeVar("T_ID", bound=Hashable)
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)
_COMPILED_PATTERN: dict[str, re.Pattern] = {}


class Loader(ABC):
    """This is the base class for all loaders

    Args:
        client (CogniteClient): The client to use for interacting with the CDF API.
        build_dir (Path): The path to the build directory

    Class attributes:
        filetypes: The filetypes that are supported by this loader. This should be set in all subclasses.
        folder_name: The name of the folder in the build directory where the files are located. This should be set in all subclasses.
        filename_pattern: A regex pattern that is used to filter the files that are supported by this loader. This is used
            when two loaders have the same folder name to differentiate between them. If not set, all files are supported.
        dependencies: A set of loaders that must be loaded before this loader.
        exclude_filetypes: A set of filetypes that should be excluded from the supported filetypes.
    """

    filetypes: frozenset[str]
    folder_name: str
    kind: str
    filename_pattern: str = ""
    dependencies: frozenset[type[ResourceLoader]] = frozenset()
    exclude_filetypes: frozenset[str] = frozenset()
    _doc_base_url: str = "https://api-docs.cognite.com/20230101/tag/"
    _doc_url: str = ""

    def __init__(self, client: ToolkitClient, build_dir: Path | None):
        self.client: ToolkitClient = client
        self.resource_build_path: Path | None = None
        if build_dir is not None and build_dir.name == self.folder_name:
            raise ValueError(f"Build directory cannot be the same as the resource folder name: {self.folder_name}")
        elif build_dir is not None:
            self.resource_build_path = build_dir / self.folder_name
        self.extra_configs: dict[str, Any] = {}

    @classmethod
    def create_loader(cls: type[T_Loader], ToolGlobals: CDFToolConfig, build_dir: Path | None) -> T_Loader:
        return cls(ToolGlobals.toolkit_client, build_dir)

    @property
    def display_name(self) -> str:
        return self.folder_name

    @classmethod
    def doc_url(cls) -> str:
        return cls._doc_base_url + cls._doc_url

    def find_files(self, dir_or_file: Path | None = None) -> list[Path]:
        """Find all files that are supported by this loader in the given directory or file.

        Args:
            dir_or_file (Path): The directory or file to search in. If no path is given,
                the build directory is used.


        Returns:
            list[Path]: A sorted list of all files that are supported by this loader.

        """
        dir_or_file = dir_or_file or self.resource_build_path
        if dir_or_file is None:
            raise ValueError("No 'dir_or_file' or 'build_path' is set.")
        if dir_or_file.is_file():
            if not self.is_supported_file(dir_or_file):
                raise ValueError("Invalid file type")
            return [dir_or_file]
        elif dir_or_file.is_dir():
            file_paths = [file for file in dir_or_file.glob("**/*") if self.is_supported_file(file)]
            return sorted(file_paths)
        else:
            return []

    @classmethod
    def any_supported_files(cls, directory: Path) -> bool:
        return any(cls.is_supported_file(file) for file in directory.glob("**/*"))

    @classmethod
    def is_supported_file(cls, file: Path) -> bool:
        if cls.filetypes and file.suffix[1:] not in cls.filetypes:
            return False
        if cls.exclude_filetypes and file.suffix[1:] in cls.exclude_filetypes:
            return False
        if cls.filename_pattern:
            if cls.filename_pattern not in _COMPILED_PATTERN:
                _COMPILED_PATTERN[cls.filename_pattern] = re.compile(cls.filename_pattern, re.IGNORECASE)
            return _COMPILED_PATTERN[cls.filename_pattern].match(file.stem) is not None
        return True


T_Loader = TypeVar("T_Loader", bound=Loader)


class ResourceLoader(
    Loader,
    ABC,
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
):
    """This is the base class for all resource loaders.

    A resource loader consists of the following
        - A CRUD (Create, Retrieve, Update, Delete) interface for interacting with the CDF API.
        - A read and write data class with list for the resource.
        - Must use the file-format YAML to store the local version of the resource.

    All resources supported by the cognite_toolkit should implement a loader.

    Class attributes:
        resource_write_cls: The write data class for the resource.
        resource_cls: The read data class for the resource.
        list_cls: The read list format for this resource.
        list_write_cls: The write list format for this resource.
        support_drop: Whether the resource supports the drop flag.
        filetypes: The filetypes that are supported by this loader. This should not be set in the subclass, it
            should always be yaml and yml.
        dependencies: A set of loaders that must be loaded before this loader.
    """

    # Must be set in the subclass
    resource_write_cls: type[T_WriteClass]
    resource_cls: type[T_WritableCogniteResource]
    list_cls: type[T_WritableCogniteResourceList]
    list_write_cls: type[T_CogniteResourceList]
    # Optional to set in the subclass
    support_drop = True
    filetypes = frozenset({"yaml", "yml"})
    dependencies: frozenset[type[ResourceLoader]] = frozenset()

    # The methods that must be implemented in the subclass
    @classmethod
    @abstractmethod
    def get_id(cls, item: T_WriteClass | T_WritableCogniteResource | dict) -> T_ID:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_required_capability(cls, items: T_CogniteResourceList) -> Capability | list[Capability]:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")

    @abstractmethod
    def create(self, items: T_CogniteResourceList) -> Sized:
        raise NotImplementedError

    @abstractmethod
    def retrieve(self, ids: SequenceNotStr[T_ID]) -> T_WritableCogniteResourceList:
        raise NotImplementedError

    @abstractmethod
    def update(self, items: T_CogniteResourceList) -> Sized:
        raise NotImplementedError

    @abstractmethod
    def delete(self, ids: SequenceNotStr[T_ID]) -> int:
        raise NotImplementedError

    @abstractmethod
    def iterate(self) -> Iterable[T_WritableCogniteResource]:
        raise NotImplementedError

    # The methods below have default implementations that can be overwritten in subclasses
    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        return read_parameter_from_init_type_hints(cls.resource_write_cls).as_camel_case()

    @classmethod
    def check_identifier_semantics(
        cls, identifier: T_ID, filepath: Path, verbose: bool
    ) -> WarningList[YAMLFileWarning]:
        """This should be overwritten in subclasses to check the semantics of the identifier."""
        return WarningList[YAMLFileWarning]()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        return
        yield

    @classmethod
    def create_empty_of(cls, items: T_CogniteResourceList) -> T_CogniteResourceList:
        return cls.list_write_cls([])

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> T_WriteClass | T_CogniteResourceList | None:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_write_cls.load(raw_yaml)
        else:
            return self.list_write_cls([self.resource_write_cls.load(raw_yaml)])

    def dump_resource(
        self, resource: T_WriteClass, source_file: Path, local_resource: T_WriteClass
    ) -> tuple[dict[str, Any], dict[Path, str]]:
        """Dumps the resource to a dictionary that matches the write format.

        In addition, it can return a dictionary with extra files and their content. This is, for example, used by
        Transformations to dump the 'query' key to an .sql file.

        Args:
            resource (T_WritableCogniteResource): The resource to dump (typically comes from CDF).
            source_file (Path): The source file that the resource was loaded from.
            local_resource (T_WritableCogniteResource): The local resource.

        Returns:
            tuple[dict[str, Any], dict[Path, str]]: The dumped resource and a dictionary with extra files and their
             content.
        """
        return resource.dump(), {}

    @overload
    def are_equal(
        self, local: T_WriteClass, cdf_resource: T_WritableCogniteResource, return_dumped: Literal[False] = False
    ) -> bool: ...

    @overload
    def are_equal(
        self, local: T_WriteClass, cdf_resource: T_WritableCogniteResource, return_dumped: Literal[True]
    ) -> tuple[bool, dict[str, Any], dict[str, Any]]: ...

    def are_equal(
        self, local: T_WriteClass, cdf_resource: T_WritableCogniteResource, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        return self._are_equal(local, cdf_resource, return_dumped)

    # Private to avoid having to overload in all subclasses
    def _are_equal(
        self, local: T_WriteClass, cdf_resource: T_WritableCogniteResource, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        """This can be overwritten in subclasses that require special comparison logic.

        For example, TransformationWrite has OIDC credentials that will not be returned
        by the retrieve method, and thus needs special handling.
        """
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    @staticmethod
    def _return_are_equal(
        local_dumped: dict[str, Any], cdf_dumped: dict[str, Any], return_dumped: bool
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        if return_dumped:
            return local_dumped == cdf_dumped, local_dumped, cdf_dumped
        else:
            return local_dumped == cdf_dumped

    # Helper method
    @classmethod
    def get_ids(cls, items: Sequence[T_WriteClass | T_WritableCogniteResource | dict]) -> list[T_ID]:
        return [cls.get_id(item) for item in items]


class ResourceContainerLoader(
    ResourceLoader[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    ABC,
):
    """This is the base class for all loaders' resource containers.

    A resource container is a resource that contains data. For example, Timeseries contains datapoints, and another
    example is spaces and containers in data modeling that contains instances.

    In addition to the methods that are required for a resource loader, a resource container loader must implement
    the following methods:
        - count: Counts the number of items in the resource container.
        - drop_data: Deletes the data in the resource container.

    class attributes:
        item_name: The name of the item that is stored in the resource container. This should be set in the subclass.
            It is used to display messages when running operations.
    """

    item_name: str

    @abstractmethod
    def count(self, ids: SequenceNotStr[T_ID]) -> int:
        raise NotImplementedError

    @abstractmethod
    def drop_data(self, ids: SequenceNotStr[T_ID]) -> int:
        raise NotImplementedError


class DataLoader(Loader, ABC):
    """This is the base class for all data loaders.

    A data loader is a loader that uploads data to CDF. It will typically depend on a
    resource container that stores the data. For example, the datapoints loader depends
    on the timeseries loader.

    It has only one required method:
        - upload: Uploads the data to CDF.

    class attributes:
        item_name: The name of the item that is stored in the resource container. This should be set in the subclass.
            It is used to display messages when running operations.

    """

    item_name: str

    @abstractmethod
    def upload(self, datafile: Path, ToolGlobals: CDFToolConfig, dry_run: bool) -> tuple[str, int]:
        raise NotImplementedError
