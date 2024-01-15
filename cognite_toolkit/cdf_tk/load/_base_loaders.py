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
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit.cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

from .data_classes import (
    DatapointDeployResult,
    DeployResult,
    RawDatabaseTable,
    ResourceDeployResult,
    UploadDeployResult,
)

T_ID = TypeVar("T_ID", bound=Union[str, int, DataModelingId, InstanceId, VersionedDataModelingId, RawDatabaseTable])
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)


class Loader(ABC):
    """This is the base class for all loaders

    Args:
        client (CogniteClient): The client to use for interacting with the CDF API.

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
    filename_pattern: str = ""
    dependencies: frozenset[type[ResourceLoader]] = frozenset()
    exclude_filetypes: frozenset[str] = frozenset()

    def __init__(self, client: CogniteClient):
        self.client = client

    @classmethod
    def create_loader(cls: type[T_Loader], ToolGlobals: CDFToolConfig) -> T_Loader:
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return cls(client)

    @classmethod
    @abstractmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")

    @property
    def display_name(self) -> str:
        return self.folder_name

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
                file_paths = (file for file in file_paths if pattern.match(file.stem))

            if cls.exclude_filetypes:
                file_paths = (file for file in file_paths if file.suffix[1:] not in cls.exclude_filetypes)

            return list(file_paths)
        else:
            return []

    @abstractmethod
    def deploy_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        drop: bool = False,
        clean: bool = False,
        dry_run: bool = False,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> DeployResult | None:
        raise NotImplementedError


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
        api_name: The name of the api that is in the cognite_client that is used to interact with the CDF API.
        resource_write_cls: The write data class for the resource.
        resource_cls: The read data class for the resource.
        list_cls: The read list format for this resource.
        list_write_cls: The write list format for this resource.
        support_drop: Whether the resource supports the drop flag.
        filetypes: The filetypes that are supported by this loader. This should not be set in the subclass, it
            should always be yaml and yml.
        identifier_key: The key that is used to identify the resource. This should be set in the subclass.
        dependencies: A set of loaders that must be loaded before this loader.
        _display_name: The name of the resource that is used when printing messages. If this is not set, the
            api_name is used.
    """

    # Must be set in the subclass
    api_name: str
    resource_write_cls: type[T_WriteClass]
    resource_cls: type[T_WritableCogniteResource]
    list_cls: type[T_WritableCogniteResourceList]
    list_write_cls: type[T_CogniteResourceList]
    # Optional to set in the subclass
    support_drop = True
    filetypes = frozenset({"yaml", "yml"})
    identifier_key: str = "externalId"
    dependencies: frozenset[type[ResourceLoader]] = frozenset()
    _display_name: str = ""

    def __init__(self, client: CogniteClient):
        super().__init__(client)
        try:
            self.api_class = self._get_api_class(client, self.api_name)
        except AttributeError:
            raise AttributeError(f"Invalid api_name {self.api_name}.")

    @property
    def display_name(self) -> str:
        return self._display_name or self.api_name

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
    @abstractmethod
    def get_id(cls, item: T_WriteClass | T_WritableCogniteResource) -> T_ID:
        raise NotImplementedError

    @classmethod
    def get_ids(cls, items: Sequence[T_WriteClass | T_WritableCogniteResource]) -> list[T_ID]:
        return [cls.get_id(item) for item in items]

    # Default implementations that can be overridden
    @classmethod
    def create_empty_of(cls, items: T_CogniteResourceList) -> T_CogniteResourceList:
        return cls.list_write_cls([])

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> T_WriteClass | T_CogniteResourceList:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_write_cls.load(raw_yaml)
        return self.resource_write_cls.load(raw_yaml)

    def create(self, items: T_CogniteResourceList) -> Sized:
        return self.api_class.create(items)

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

    def update(self, items: T_CogniteResourceList) -> Sized:
        return self.api_class.update(items)

    def delete(self, ids: SequenceNotStr[T_ID]) -> int:
        self.api_class.delete(ids)
        return len(ids)

    def deploy_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        drop: bool = False,
        clean: bool = False,
        dry_run: bool = False,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult | None:
        filepaths = self.find_files(path)

        batches = self._load_batches(filepaths, ToolGlobals, skip_validation=dry_run)
        if batches is None:
            ToolGlobals.failed = True
            return None

        nr_of_batches = len(batches)
        nr_of_items = sum(len(batch) for batch in batches)
        if nr_of_items == 0:
            return ResourceDeployResult(name=self.display_name)

        action_word = "Loading" if dry_run else "Uploading"
        print(f"[bold]{action_word} {nr_of_items} {self.display_name} in {nr_of_batches} batches to CDF...[/]")

        if drop and self.support_drop:
            if drop_data and (self.api_name in ["data_modeling.spaces", "data_modeling.containers"]):
                print(
                    f"  --drop-data is specified, will delete existing nodes and edges before before deleting {self.display_name}."
                )
            else:
                print(f"  --drop is specified, will delete existing {self.display_name} before uploading.")

        # Deleting resources.
        nr_of_deleted = 0
        if (drop and self.support_drop) or clean:
            nr_of_deleted = self._delete_resources(batches, drop_data, dry_run, verbose)

        nr_of_created = nr_of_changed = nr_of_unchanged = 0
        for batch_no, batch in enumerate(batches, 1):
            to_create, to_update, unchanged = self._to_create_changed_unchanged_triple(batch)

            nr_of_unchanged += len(unchanged)
            if dry_run:
                nr_of_created += len(to_create)
                nr_of_changed += len(to_update)
                if verbose:
                    print(
                        f" {batch_no}/{len(batch)} {self.display_name} would have: Changed {nr_of_changed},"
                        f" Created {nr_of_created}, and left {len(unchanged)} unchanged"
                    )
                continue

            if to_create:
                created = self._create_resources(to_create)
                if created is None:
                    ToolGlobals.failed = True
                    return None
                nr_of_created += created

            if to_update:
                updated = self._update_resources(to_update)
                if updated is None:
                    ToolGlobals.failed = True
                    return None
                nr_of_changed += updated

        if verbose:
            print(
                f"  Created {nr_of_created}, Deleted {nr_of_deleted}, Changed {nr_of_changed}, Unchanged {nr_of_unchanged}, Total {nr_of_items}."
            )
        return ResourceDeployResult(
            name=self.display_name,
            created=nr_of_created,
            deleted=nr_of_deleted,
            changed=nr_of_changed,
            unchanged=nr_of_unchanged,
            total=nr_of_items,
        )

    def clean_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        dry_run: bool = False,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult | None:
        filepaths = self.find_files(path)

        # Since we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
        batches = self._load_batches(filepaths, ToolGlobals, skip_validation=dry_run)
        if batches is None:
            ToolGlobals.failed = True
            return None

        nr_of_batches = len(batches)
        nr_of_items = sum(len(batch) for batch in batches)
        if nr_of_items == 0:
            return ResourceDeployResult(name=self.display_name)

        action_word = "Loading" if dry_run else "Cleaning"
        print(f"[bold]{action_word} {nr_of_items} {self.display_name} in {nr_of_batches} batches to CDF...[/]")

        # Deleting resources.
        nr_of_deleted = self._delete_resources(batches, drop_data, dry_run, verbose)

        return ResourceDeployResult(name=self.display_name, deleted=nr_of_deleted, total=nr_of_items)

    def _to_create_changed_unchanged_triple(
        self, batch: T_CogniteResourceList
    ) -> tuple[T_CogniteResourceList, T_CogniteResourceList, T_CogniteResourceList]:
        batch_ids = self.get_ids(batch)
        to_create, to_update, unchanged = (
            self.create_empty_of(batch),
            self.create_empty_of(batch),
            self.create_empty_of(batch),
        )
        try:
            cdf_resources = self.retrieve(batch_ids)
        except Exception:
            print(f"  [bold yellow]WARNING:[/] Failed to retrieve {len(batch_ids)} of {self.display_name}.")
            cdf_resource_by_id = {}
        else:
            cdf_resource_by_id = {self.get_id(resource): resource for resource in cdf_resources.as_write()}

        for item in batch:
            cdf_resource = cdf_resource_by_id.get(self.get_id(item))
            if cdf_resource and item == cdf_resource:
                unchanged.append(item)
            elif cdf_resource:
                to_update.append(item)
            else:
                to_create.append(item)
        return to_create, to_update, unchanged

    def _load_batches(
        self, filepaths: list[Path], ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> list[T_CogniteResourceList] | None:
        batches: list[T_CogniteResourceList] = []
        for filepath in filepaths:
            try:
                resource = self.load_resource(filepath, ToolGlobals, skip_validation)
            except KeyError as e:
                # KeyError means that we are missing a required field in the yaml file.
                print(
                    f"[bold red]ERROR:[/] Failed to load {filepath.name} with {self.display_name}. Missing required field: {e}."
                )
                return None
            if resource is None:
                print(f"[bold yellow]WARNING:[/] Skipping {filepath.name}. No data to load.")
                continue
            batches.append(resource if isinstance(resource, self.list_write_cls) else self.list_write_cls([resource]))
        return batches

    def _delete_resources(
        self, batches: list[T_CogniteResourceList], drop_data: bool, dry_run: bool, verbose: bool
    ) -> int:
        nr_of_deleted = 0
        for batch in batches:
            batch_ids = self.get_ids(batch)
            if dry_run:
                nr_of_deleted += len(batch_ids)
                if verbose:
                    print(f"  Would have deleted {len(batch_ids)} {self.display_name}.")
                continue

            if isinstance(self, ResourceContainerLoader) and drop_data:
                self.drop_data(batch_ids)
            elif isinstance(self, ResourceContainerLoader):
                print(f"  [bold]INFO:[/] Skipping deletion of {self.display_name} as drop_data flag is not set...")
                continue

            try:
                nr_of_deleted += self.delete(batch_ids)
            except CogniteAPIError as e:
                if e.code == 404:
                    print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {self.display_name} do(es) not exist.")
            except CogniteNotFoundError:
                print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {self.display_name} do(es) not exist.")
            except Exception as e:
                print(f"  [bold yellow]WARNING:[/] Failed to delete {len(batch_ids)} {self.display_name}. Error {e}.")
            else:  # Delete succeeded
                if verbose:
                    print(f"  Deleted {len(batch_ids)} {self.display_name}.")
        return nr_of_deleted

    def _create_resources(self, resources: T_CogniteResourceList) -> int | None:
        try:
            created = self.create(resources)
        except CogniteAPIError as e:
            if e.code == 409:
                print("  [bold yellow]WARNING:[/] Resource(s) already exist(s), skipping creation.")
            else:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                return None
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} out of {len(resources)} resource(s) already exist(s). {len(e.successful or [])} resource(s) created."
            )
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            return None
        else:
            return len(created) if created is not None else 0
        return 0

    def _update_resources(self, resources: T_CogniteResourceList) -> int | None:
        try:
            updated = self.update(resources)
        except Exception as e:
            print(f"  [bold yellow]Error:[/] Failed to update {self.display_name}. Error {e}.")
            return None
        else:
            return len(updated)


class ResourceContainerLoader(
    ResourceLoader[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    ABC,
):
    """This is the base class for all loaders resource containers.

    A resource container is a resource that contains data. For example, Timeseries contains datapoints, and another
    example is spaces and containers in data modeling that contains instances.

    In addition to the methods that are required for a resource loader, a resource container loader must implement
    the following methods:
        - count: Counts the number of items in the resource container.
        - drop_data: Deletes the data in the resource container.
    """

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

    """

    @abstractmethod
    def upload(self, datafile: Path, dry_run: bool) -> tuple[str, int]:
        raise NotImplementedError

    def deploy_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        drop: bool = False,
        clean: bool = False,
        dry_run: bool = False,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> UploadDeployResult | None:
        filepaths = self.find_files(path)

        print(f"[bold]Uploading {len(filepaths)} data {self.display_name} files to CDF...[/]")
        datapoints = 0
        for filepath in filepaths:
            try:
                message, file_datapoints = self.upload(filepath, dry_run)
            except Exception as e:
                print(f"  [bold red]Error:[/] Failed to upload {filepath.name}. Error {e}.")
                ToolGlobals.failed = True
                return None
            if verbose:
                print(message)
            datapoints += file_datapoints
        if datapoints != 0:
            return DatapointDeployResult(self.display_name, cells=datapoints, uploaded=len(filepaths))
        else:
            return UploadDeployResult(self.display_name, uploaded=len(filepaths))
