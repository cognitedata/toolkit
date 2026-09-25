import sys
from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable, Sequence, Sized
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import (
    Identifier,
    T_Identifier,
    T_RequestResource,
    T_ResponseResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import AclType
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING, YAML_SUFFIX
from cognite_toolkit._cdf_tk.utils import load_yaml_inject_variables, safe_read, sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable, ResourceType

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


T_YamlResource = TypeVar("T_YamlResource")


class ReadExtra(BaseModel):
    source_path: Path


class FailedReadExtra(ReadExtra):
    code: Literal["MISSING", "SYNTAX-ERROR"]
    error: str


class SuccessExtra(ReadExtra):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    is_list: bool = Field(False, description="Whether the extra content is a list of items or a single item.")
    resource_field: str | None = Field(
        description="Name of the resource field where the "
        "extra content should be placed in the resource. For example, in transformations this is 'query'."
        "If None, the extra content needs to be written to a separate file."
    )
    remove_fields: list[str] = Field(
        description="List of fields that should be removed from the resource when writing to the build directory. For example, in transformations the 'queryFile' field should be removed as it not part of the API.",
        default_factory=list,
    )
    source_hash: str
    suffix: str
    content_parsed: dict[str, Any] | list[Any] | None = None
    content: str | None = None
    content_byte: bytes | None = None
    description: str
    write_to_build: bool = Field(
        False, description="Whether the extra content should be written to the build directory."
    )


class ResourceIO(
    ABC,
    Generic[T_Identifier, T_RequestResource, T_ResponseResource, T_YamlResource],
):
    """This is the base class for all resources input/output to CDF and file.

    A resource IO consists of the following
        - A CRUD (Create, Retrieve, Update, Delete) interface for interacting with the CDF API.
        - Serialization/Deserialization of the resources between YAML and the cognite-sdk data classes used in the
          CRUD interface.

    All resources supported by the cognite_toolkit should implement a CRUD.

    Class attributes:
        resource_write_cls: The API write data class for the resource.
        resource_cls: The API read data class for the resource.
        yaml_cls: The File format for this resource. This is used to validate the user input.
        folder_name: The name of the folder in the build directory where the files are located. This should be set in all subclasses.
        kind: The stem of the resource type
        sub_folder_name: The name of the default subfolder name in the resource directory. This is used, for example,
            when dumping data models to put containers and views into subdirectories.
        extra_kinds: These are kinds of extras. For example, in the functions folders, CogniteFile and FileMetadata is allowed
            as these contain function code.
        support_drop: Whether the resource supports the drop flag.
        support_update: Whether the resource supports the update operation.
        dependencies: A set of other resource resource_ios that must be loaded before this crud.
        parent_resource: A set of other resource resource_ios that are parent resources to this resource. This is used
            to determine if the iterate method should return any resources when filtering by parent ids.
    """

    # Must be set in the subclass
    resource_write_cls: type[T_RequestResource]
    resource_cls: type[T_ResponseResource]
    yaml_cls: type[ToolkitResource]
    folder_name: str
    kind: str

    # Optional to set in the subclass
    _doc_base_url: str = "https://api-docs.cognite.com/20230101/tag/"
    _doc_url: str = ""
    sub_folder_name: str | None = None
    extra_kinds: frozenset[str] = frozenset()
    support_drop = True
    support_update = True
    drop_confirmation_message: ClassVar[str | None] = None
    dependencies: "frozenset[type[ResourceIO]]" = frozenset()
    # For example, TransformationNotification and Schedule has Transformation as the parent resource
    # This is used in the iterate method to ensure that nothing is returned if
    # the resource type does not have a parent resource.
    parent_resource: "frozenset[type[ResourceIO]]" = frozenset()

    # This is used for resources that support extra files. For example, the Transformation resources
    # supports having the query in a separate .sql file. This is the property were the extra content should be placed,
    # for example, the Transformation resource has the query property that is used to store the query content.
    extra_content_property: str | None = None

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self.console = client.console

    # The methods that must be implemented in the subclass
    @classmethod
    @abstractmethod
    def get_id(cls, item: T_RequestResource | T_ResponseResource | dict) -> T_Identifier:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def dump_id(cls, id: T_Identifier) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def create_minimum_acl(
        cls, actions: set[Literal["READ", "WRITE"]], items: Sequence[T_RequestResource]
    ) -> Iterable[AclType]:
        if minimum_scope := cls.get_minimum_scope(items):
            yield from cls.create_acl(actions, minimum_scope)

    @classmethod
    @abstractmethod
    def get_minimum_scope(cls, items: Sequence[T_RequestResource]) -> ScopeDefinition | None:
        raise NotImplementedError(f"get_minimum_scope must be implemented for {cls.__name__}.")

    @classmethod
    @abstractmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        raise NotImplementedError(f"create_acl must be implemented for {cls.__name__} ")

    @abstractmethod
    def create(self, items: Sequence[T_RequestResource]) -> Sized:
        raise NotImplementedError

    @abstractmethod
    def retrieve(self, ids: Sequence[T_Identifier]) -> Sequence[T_ResponseResource]:
        raise NotImplementedError

    def update(self, items: Sequence[T_RequestResource]) -> Sized:
        raise NotImplementedError(f"Update is not supported for {type(self).__name__}.")

    @abstractmethod
    def delete(self, ids: Sequence[T_Identifier]) -> int:
        raise NotImplementedError

    def iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[T_ResponseResource]:
        if sum([1 for x in [data_set_external_id, space, parent_ids] if x is not None]) > 1:
            raise ValueError("At most one of data_set_external_id, space, or parent_ids must be set.")
        if parent_ids is not None and not self.parent_resource:
            return []
        if space is not None:
            from ._datamodel import SpaceCRUD

            if SpaceCRUD not in self.dependencies:
                return []
        if data_set_external_id is not None:
            from ._data_organization import DataSetsIO

            if DataSetsIO not in self.dependencies:
                return []
        return self._iterate(data_set_external_id, space, parent_ids)

    @abstractmethod
    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[T_ResponseResource]:
        raise NotImplementedError

    ### These methods can be optionally overwritten in the subclass ###
    def prerequisite_warning(self) -> str | None:
        """ "Returns a warning message if there are any prerequisites that must be met before using this CRUD.

        This is used for special resource CRUDs that for example require data models/views to be deployed in CDF
        to work. For example, the InfieldV1CRUD and the ResourceViewMappingCRUD.
        """
        return None

    @classmethod
    @abstractmethod
    def get_dependencies(cls, resource: T_YamlResource) -> "Iterable[tuple[type[ResourceIO], Identifier]]":
        """Returns dependencies for a given resource.
        This is used to determine the order of deployment and to check for missing dependencies.

        Args:
            resource: The resource to get dependencies for.

        """
        raise NotImplementedError(f"get_dependencies must be implemented for {cls.__name__}.")

    @classmethod
    def safe_read(cls, filepath: Path | str) -> str:
        """Reads the file and returns the content. This is intended to be overwritten in subclasses that require special
        handling of the files content. For example, Data Models need to quote the value on the version key to ensure
        it is parsed as a string."""
        return safe_read(filepath, encoding=BUILD_FOLDER_ENCODING)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        """Loads the resource(s) from a file. Can be overwritten in subclasses.

        Examples, is the TransformationLoader that loads the query from a file. Another example, is the View and
        DataModel loaders that nees special handling of the yaml to ensure version key is parsed as a string.
        """
        raw_yaml = load_yaml_inject_variables(
            self.safe_read(filepath),
            environment_variables or {},
            original_filepath=filepath,
        )
        return raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> T_RequestResource:
        """Loads the resource from a dictionary. Can be overwritten in subclasses."""
        return self.resource_write_cls._load(resource)

    def dump_resource(self, resource: T_ResponseResource, local: dict[str, Any] | None = None) -> dict[str, Any]:
        """Dumps the resource to a dictionary that matches the write format.

        This is intended to be overwritten in subclasses that require special dumping logic, for example,
        replacing dataSetId with dataSetExternalId.

        Args:
            resource (T_ResponseResource): The resource to dump (typically comes from CDF).
            local (dict[str, Any] | None): The local resource. When used in a dump/import command, there is no local
                resource.
        """
        return resource.as_write().dump()

    def split_resource(
        self, base_filepath: Path, resource: dict[str, Any]
    ) -> Iterable[tuple[Path, dict[str, Any] | str]]:
        """Splits a resource into multiple files.

        This is used in the dump command. For example, a transformation can be split into the YAML file and
        the SQL file for the query

        Args:
            base_filepath (Path): The base filepath to use when creating the new file.
            resource (dict[str,Any]): The resource to split.

        Returns:
            Iterable[[dict[str,Any], Path]]: An iterable of the new resources and the filepaths where they should be
                saved.
        """
        return [(base_filepath, resource)]

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        """Diff two lists and return the indices that needs to be compared and the indices that have been added.
        The lists are subfields of the local and CDF resources.

        This is used by the pull command to determine changes to the local resources compared to the CDF resources. For
        example, a Sequence has a list of columns. This method is used to determine which columns to compare
        and which has been added.

        Args:
            local (list[Any]): The local list.
            cdf (list[Any]): The CDF list.
            json_path (tuple[str | int, ...]): The json path to the list in the resource. For example, 'columns'
                in the case of a sequence.

        Returns:
            tuple[dict[int, int], list[int]]: A dictionary with the indices that needs to be compared and a list of
                indices that have been added. The dictionary has local index as key and CDF index as value. The
                list of indices that have been added are cdf indices.
        """
        raise NotImplementedError(
            f"Missing implementation for {type(self).__name__} for {'.'.join(map(str, json_path))}."
        )

    def sensitive_strings(self, item: T_RequestResource) -> Iterable[str]:
        """Returns a list of strings that should be masked when printing.

        This is used by the loaders with credentials to mask the credentials secrets. For example, the
        WorkflowTriggerLoader uses this to maks the clientSecret.
        """
        return
        yield

    # Helper methods
    @classmethod
    def as_resource_type(cls) -> "ResourceType":
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ResourceType

        return ResourceType(kind=cls.kind, resource_folder=cls.folder_name)

    @classmethod
    def create_io(cls, client: ToolkitClient) -> Self:
        return cls(client)

    @property
    def display_name(self) -> str:
        return self.folder_name

    @classmethod
    def doc_url(cls) -> str:
        return cls._doc_base_url + cls._doc_url

    def find_files(self, dir_or_file: Path) -> list[Path]:
        """Find all files that are supported by this loader in the given directory or file.

        Args:
            dir_or_file (Path): The directory or file to search in. If no path is given,
                the build directory is used.

        Returns:
            list[Path]: A sorted list of all files that are supported by this loader.

        """
        dir_or_file = dir_or_file
        if dir_or_file is None:
            raise ValueError("No 'dir_or_file' or 'build_path' is set.")
        if dir_or_file.is_file():
            if not self.is_supported_file(dir_or_file):
                raise ValueError("Invalid file type")
            return [dir_or_file]
        elif dir_or_file.is_dir():
            return sorted([file for file in dir_or_file.rglob("*") if self.is_supported_file(file)])
        else:
            return []

    @classmethod
    def is_supported_file(cls, file: Path) -> bool:
        """Check if hte file is supported by this loader.

        Args:
            file: The filepath to check.

        Returns:
            bool: True if the file is supported, False otherwise.

        """
        return file.suffix in YAML_SUFFIX and file.stem.casefold().endswith(cls.kind.casefold())

    @classmethod
    def get_ids(cls, items: Sequence[T_RequestResource | T_ResponseResource | dict]) -> list[T_Identifier]:
        return [cls.get_id(item) for item in items]

    @classmethod
    def as_str(cls, id: T_Identifier) -> str:
        if isinstance(id, str):
            return sanitize_filename(id)
        raise NotImplementedError(
            f"Bug in CogniteToolkit 'as_str' is not implemented for {cls.__name__.removesuffix('Loader')}."
        )

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: T_Identifier, item: dict[str, Any]) -> Iterable[ReadExtra]:
        yield from ()

    @classmethod
    def substitute_variables_content(cls, content: str, variables: "list[BuildVariable]") -> str:
        """Variable substitution in the content of a file. This is used in the build command.

        This is overwritten in the TransformationIO to handle substitution in the query field.
        """
        # To avoid circular import
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable

        return BuildVariable.substitute(content, variables, ".yaml")

    def load_resource_files(
        self,
        filepaths: list[Path],
        environment_variables: dict[str, str | None] | None = None,
        is_dry_run: bool = False,
    ) -> list[T_RequestResource]:
        """Loads the resources from the given filepaths.

        This is a convenience method that combines .load_resource_file and load_resource to load all resources
        from the given filepaths.
        """
        request_items: list[T_RequestResource] = []
        for file in filepaths:
            raw = self.load_resource_file(file, environment_variables)
            for item in raw:
                request_items.append(self.load_resource(item, is_dry_run))
        return request_items


class ResourceContainerIO(ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource, T_YamlResource], ABC):
    """This is the base class for all resource CRUD' containers.

    A resource container CRUD is a resource that contains data. For example, Timeseries contains datapoints, and another
    example is spaces and containers in data modeling that contains instances.

    In addition to the methods that are required for a resource CRUD, a resource container CRUD must implement
    the following methods:
        - count: Counts the number of items in the resource container.
        - drop_data: Deletes the data in the resource container.

    class attributes:
        item_name: The name of the item that is stored in the resource container. This should be set in the subclass.
            It is used to display messages when running operations.
    """

    item_name: str

    @abstractmethod
    def count(self, ids: Sequence[T_Identifier]) -> int:
        raise NotImplementedError

    @abstractmethod
    def drop_data(self, ids: Sequence[T_Identifier]) -> int:
        raise NotImplementedError
