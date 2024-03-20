from __future__ import annotations

import inspect
import re
import traceback
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
from rich.panel import Panel

from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

from .data_classes import (
    DatapointDeployResult,
    DeployResult,
    RawDatabaseTable,
    ResourceContainerDeployResult,
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

    def __init__(self, client: CogniteClient, build_path: Path | None = None):
        self.client = client
        self.build_path = build_path
        self.extra_configs: dict[str, Any] = {}

    @classmethod
    def create_loader(cls: type[T_Loader], ToolGlobals: CDFToolConfig) -> T_Loader:
        return cls(ToolGlobals.client)

    @property
    def display_name(self) -> str:
        return self.folder_name

    @classmethod
    def find_files(cls, dir_or_file: Path) -> list[Path]:
        """Find all files that are supported by this loader in the given directory or file.

        Args:
            dir_or_file (Path): The directory or file to search in.

        Returns:
            list[Path]: A sorted list of all files that are supported by this loader.

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

            return sorted(list(file_paths))
        else:
            return []

    @abstractmethod
    def deploy_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        dry_run: bool = False,
        has_done_drop: bool = False,
        has_dropped_data: bool = False,
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
    @abstractmethod
    def get_required_capability(cls, items: T_CogniteResourceList) -> Capability | list[Capability]:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")

    @classmethod
    def get_ids(cls, items: Sequence[T_WriteClass | T_WritableCogniteResource]) -> list[T_ID]:
        return [cls.get_id(item) for item in items]

    # Default implementations that can be overridden
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
        existing = self.retrieve(ids)
        if existing:
            self.api_class.delete(ids)
        return len(existing)

    def deploy_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        dry_run: bool = False,
        has_done_drop: bool = False,
        has_dropped_data: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult | None:
        self.build_path = path
        filepaths = self.find_files(path)

        def sort_key(p: Path) -> int:
            if result := re.findall(r"^(\d+)", p.stem):
                return int(result[0])
            else:
                return len(filepaths)

        # In the build step, the resource files are prefixed a number that controls the order in which
        # the resources are deployed. The custom 'sort_key' here is to get a sort on integer instead of a default string
        # sort.
        filepaths = sorted(filepaths, key=sort_key)

        loaded_resources = self._load_files(filepaths, ToolGlobals, skip_validation=dry_run, verbose=verbose)
        if loaded_resources is None:
            ToolGlobals.failed = True
            return None

        # Duplicates should be handled on the build step,
        # but in case any of them slip through, we do it here as well to
        # avoid an error.
        loaded_resources, duplicates = self._remove_duplicates(loaded_resources)

        if not loaded_resources:
            return ResourceDeployResult(name=self.display_name)

        capabilities = self.get_required_capability(loaded_resources)
        if capabilities:
            ToolGlobals.verify_capabilities(capabilities)

        nr_of_items = len(loaded_resources)
        if nr_of_items == 0:
            return ResourceDeployResult(name=self.display_name)

        prefix = "Would deploy" if dry_run else "Deploying"
        print(f"[bold]{prefix} {nr_of_items} {self.display_name} to CDF...[/]")
        # Moved here to avoid printing before the above message.
        for duplicate in duplicates:
            print(f"  [bold yellow]WARNING:[/] Skipping duplicate {self.display_name} {duplicate}.")

        nr_of_created = nr_of_changed = nr_of_unchanged = 0
        to_create, to_update, unchanged = self.to_create_changed_unchanged_triple(loaded_resources)

        if dry_run:
            if (
                self.support_drop
                and has_done_drop
                and (not isinstance(self, ResourceContainerLoader) or has_dropped_data)
            ):
                # Means the resources will be deleted and not left unchanged or changed
                for item in unchanged:
                    # We cannot use extents as LoadableNodes cannot be extended.
                    to_create.append(item)
                for item in to_update:
                    to_create.append(item)
                unchanged.clear()
                to_update.clear()

            nr_of_unchanged += len(unchanged)
            nr_of_created += len(to_create)
            nr_of_changed += len(to_update)
        else:
            nr_of_unchanged += len(unchanged)

            if to_create:
                created = self._create_resources(to_create, verbose)
                if created is None:
                    ToolGlobals.failed = True
                    return None
                nr_of_created += created

            if to_update:
                updated = self._update_resources(to_update, verbose)
                if updated is None:
                    ToolGlobals.failed = True
                    return None

                nr_of_changed += updated

        if verbose:
            self._verbose_print(to_create, to_update, unchanged, dry_run)

        if isinstance(self, ResourceContainerLoader):
            return ResourceContainerDeployResult(
                name=self.display_name,
                created=nr_of_created,
                changed=nr_of_changed,
                unchanged=nr_of_unchanged,
                total=nr_of_items,
                item_name=self.item_name,
            )
        else:
            return ResourceDeployResult(
                name=self.display_name,
                created=nr_of_created,
                changed=nr_of_changed,
                unchanged=nr_of_unchanged,
                total=nr_of_items,
            )

    def clean_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        dry_run: bool = False,
        drop: bool = True,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult | None:
        if not isinstance(self, ResourceContainerLoader) and not drop:
            # Skipping silently as this, we will not drop data or delete this resource
            return ResourceDeployResult(name=self.display_name)
        if not self.support_drop:
            print(f"  [bold green]INFO:[/] {self.display_name!r} cleaning is not supported, skipping...")
            return ResourceDeployResult(name=self.display_name)
        elif isinstance(self, ResourceContainerLoader) and not drop_data:
            print(
                f"  [bold]INFO:[/] Skipping cleaning of {self.display_name!r}. This is a data resource (it contains "
                f"data and is not only configuration/metadata) and therefore "
                "requires the --drop-data flag to be set to perform cleaning..."
            )
            return ResourceContainerDeployResult(name=self.display_name, item_name=self.item_name)

        filepaths = self.find_files(path)

        # Since we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
        loaded_resources = self._load_files(filepaths, ToolGlobals, skip_validation=True, verbose=verbose)
        if loaded_resources is None:
            ToolGlobals.failed = True
            return None

        # Duplicates should be handled on the build step,
        # but in case any of them slip through, we do it here as well to
        # avoid an error.
        loaded_resources, duplicates = self._remove_duplicates(loaded_resources)

        capabilities = self.get_required_capability(loaded_resources)
        if capabilities:
            ToolGlobals.verify_capabilities(capabilities)

        nr_of_items = len(loaded_resources)
        if nr_of_items == 0:
            return ResourceDeployResult(name=self.display_name)

        if drop:
            prefix = "Would clean" if dry_run else "Cleaning"
            with_data = "with data " if isinstance(self, ResourceContainerLoader) else ""
        else:
            prefix = "Would drop data from" if dry_run else "Dropping data from"
            with_data = ""
        print(f"[bold]{prefix} {nr_of_items} {self.display_name} {with_data} from CDF...[/]")
        for duplicate in duplicates:
            print(f"  [bold yellow]WARNING:[/] Skipping duplicate {self.display_name} {duplicate}.")

        # Deleting resources.
        if isinstance(self, ResourceContainerLoader) and drop_data:
            nr_of_dropped_datapoints = self._drop_data(loaded_resources, dry_run, verbose)
            if drop:
                nr_of_deleted = self._delete_resources(loaded_resources, dry_run, verbose)
            else:
                nr_of_deleted = 0
            if verbose:
                print("")
            return ResourceContainerDeployResult(
                name=self.display_name,
                deleted=nr_of_deleted,
                total=nr_of_items,
                dropped_datapoints=nr_of_dropped_datapoints,
                item_name=self.item_name,
            )
        elif not isinstance(self, ResourceContainerLoader) and drop:
            nr_of_deleted = self._delete_resources(loaded_resources, dry_run, verbose)
            if verbose:
                print("")
            return ResourceDeployResult(name=self.display_name, deleted=nr_of_deleted, total=nr_of_items)
        else:
            return ResourceDeployResult(name=self.display_name)

    def to_create_changed_unchanged_triple(
        self, resources: T_CogniteResourceList
    ) -> tuple[T_CogniteResourceList, T_CogniteResourceList, T_CogniteResourceList]:
        """Returns a triple of lists of resources that should be created, updated, and are unchanged."""
        resource_ids = self.get_ids(resources)
        to_create, to_update, unchanged = (
            self.create_empty_of(resources),
            self.create_empty_of(resources),
            self.create_empty_of(resources),
        )
        try:
            cdf_resources = self.retrieve(resource_ids)
        except Exception as e:
            print(
                f"  [bold yellow]WARNING:[/] Failed to retrieve {len(resource_ids)} of {self.display_name}. Proceeding assuming not data in CDF. Error {e}."
            )
            print(Panel(traceback.format_exc()))
            cdf_resource_by_id = {}
        else:
            cdf_resource_by_id = {self.get_id(resource): resource for resource in cdf_resources}

        for item in resources:
            cdf_resource = cdf_resource_by_id.get(self.get_id(item))
            # The custom compare is needed when the regular == does not work. For example, TransformationWrite
            # have OIDC credentials that will not be returned by the retrieve method, and thus need special handling.
            if cdf_resource and (item == cdf_resource.as_write() or self._is_equal_custom(item, cdf_resource)):
                unchanged.append(item)
            elif cdf_resource:
                to_update.append(item)
            else:
                to_create.append(item)
        return to_create, to_update, unchanged

    def _verbose_print(
        self,
        to_create: T_CogniteResourceList,
        to_update: T_CogniteResourceList,
        unchanged: T_CogniteResourceList,
        dry_run: bool,
    ) -> None:
        print_outs = []
        prefix = "Would have " if dry_run else ""
        if to_create:
            print_outs.append(f"{prefix}Created {self._print_ids_or_length(self.get_ids(to_create))}")
        if to_update:
            print_outs.append(f"{prefix}Updated {self._print_ids_or_length(self.get_ids(to_update))}")
        if unchanged:
            print_outs.append(
                f"{'Untouched' if dry_run else 'Unchanged'} {self._print_ids_or_length(self.get_ids(unchanged))}"
            )
        prefix_message = f" {self.display_name}: "
        if len(print_outs) == 1:
            print(f"{prefix_message}{print_outs[0]}")
        elif len(print_outs) == 2:
            print(f"{prefix_message}{print_outs[0]} and {print_outs[1]}")
        else:
            print(f"{prefix_message}{', '.join(print_outs[:-1])} and {print_outs[-1]}")

    def _is_equal_custom(self, local: T_WriteClass, cdf_resource: T_WritableCogniteResource) -> bool:
        """This method is used to compare the local and cdf resource when the default comparison fails.

        This is needed for resources that have fields that are not returned by the retrieve method, like
        for example, the OIDC credentials in Transformations.
        """
        return False

    def _load_files(
        self, filepaths: list[Path], ToolGlobals: CDFToolConfig, skip_validation: bool, verbose: bool = False
    ) -> T_CogniteResourceList | None:
        loaded_resources = self.create_empty_of(self.list_write_cls([]))
        for filepath in filepaths:
            try:
                resource = self.load_resource(filepath, ToolGlobals, skip_validation)
            except KeyError as e:
                # KeyError means that we are missing a required field in the yaml file.
                print(
                    f"[bold red]ERROR:[/] Failed to load {filepath.name} with {self.display_name}. Missing required field: {e}."
                )
                return None
            except Exception as e:
                print(f"[bold red]ERROR:[/] Failed to load {filepath.name} with {self.display_name}. Error: {e!r}.")
                if verbose:
                    print(Panel(traceback.format_exc()))
                return None
            if resource is None:
                # This is intentional. It is, for example, used by the AuthLoader to skip groups with resource scopes.
                continue
            if isinstance(resource, self.list_write_cls) and not resource:
                print(f"[bold yellow]WARNING:[/] Skipping {filepath.name}. No data to load.")
                continue

            if isinstance(resource, self.list_write_cls):
                loaded_resources.extend(resource)
            else:
                loaded_resources.append(resource)
        return loaded_resources

    def _remove_duplicates(self, loaded_resources: T_CogniteResourceList) -> tuple[T_CogniteResourceList, list[T_ID]]:
        seen: set[T_ID] = set()
        output = self.create_empty_of(loaded_resources)
        duplicates: list[T_ID] = []
        for item in loaded_resources:
            identifier = self.get_id(item)
            if identifier not in seen:
                output.append(item)
                seen.add(identifier)
            else:
                duplicates.append(identifier)
        return output, duplicates

    def _delete_resources(self, loaded_resources: T_CogniteResourceList, dry_run: bool, verbose: bool) -> int:
        nr_of_deleted = 0
        resource_ids = self.get_ids(loaded_resources)
        if dry_run:
            nr_of_deleted += len(resource_ids)
            if verbose:
                print(f"  Would have deleted {self._print_ids_or_length(resource_ids)}.")
            return nr_of_deleted

        try:
            nr_of_deleted += self.delete(resource_ids)
        except CogniteAPIError as e:
            print(f"  [bold yellow]WARNING:[/] Failed to delete {self._print_ids_or_length(resource_ids)}. Error {e}.")
            if verbose:
                print(Panel(traceback.format_exc()))
        except CogniteNotFoundError:
            if verbose:
                print(f"  [bold]INFO:[/] {self._print_ids_or_length(resource_ids)} do(es) not exist.")
        except Exception as e:
            print(f"  [bold yellow]WARNING:[/] Failed to delete {self._print_ids_or_length(resource_ids)}. Error {e}.")
            if verbose:
                print(Panel(traceback.format_exc()))
        else:  # Delete succeeded
            if verbose:
                print(f"  Deleted {self._print_ids_or_length(resource_ids)}.")
        return nr_of_deleted

    def _create_resources(self, resources: T_CogniteResourceList, verbose: bool) -> int | None:
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
            if verbose:
                print(Panel(traceback.format_exc()))
            return None
        else:
            return len(created) if created is not None else 0
        return 0

    def _update_resources(self, resources: T_CogniteResourceList, verbose: bool) -> int | None:
        try:
            updated = self.update(resources)
        except Exception as e:
            print(f"  [bold yellow]Error:[/] Failed to update {self.display_name}. Error {e}.")
            if verbose:
                print(Panel(traceback.format_exc()))
            return None
        else:
            return len(updated)

    @staticmethod
    def _print_ids_or_length(resource_ids: SequenceNotStr[T_ID], limit: int = 10) -> str:
        if len(resource_ids) == 1:
            return f"{resource_ids[0]!r}"
        elif len(resource_ids) <= limit:
            return f"{resource_ids}"
        else:
            return f"{len(resource_ids)} items"


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

    def _drop_data(self, loaded_resources: T_CogniteResourceList, dry_run: bool, verbose: bool) -> int:
        nr_of_dropped = 0
        resource_ids = self.get_ids(loaded_resources)
        if dry_run:
            resource_drop_count = self.count(resource_ids)
            nr_of_dropped += resource_drop_count
            if verbose:
                self._verbose_print_drop(resource_drop_count, resource_ids, dry_run)
            return nr_of_dropped

        try:
            resource_drop_count = self.drop_data(resource_ids)
            nr_of_dropped += resource_drop_count
        except CogniteAPIError as e:
            if e.code == 404 and verbose:
                print(f"  [bold]INFO:[/] {len(resource_ids)} {self.display_name} do(es) not exist.")
        except CogniteNotFoundError:
            return nr_of_dropped
        except Exception as e:
            print(
                f"  [bold yellow]WARNING:[/] Failed to drop {self.item_name} from {len(resource_ids)} {self.display_name}. Error {e}."
            )
            if verbose:
                print(Panel(traceback.format_exc()))
        else:  # Delete succeeded
            if verbose:
                self._verbose_print_drop(resource_drop_count, resource_ids, dry_run)
        return nr_of_dropped

    def _verbose_print_drop(self, drop_count: int, resource_ids: SequenceNotStr[T_ID], dry_run: bool) -> None:
        prefix = "Would have dropped" if dry_run else "Dropped"
        if drop_count > 0:
            print(
                f"  {prefix} {drop_count:,} {self.item_name} from {self.display_name}: "
                f"{self._print_ids_or_length(resource_ids)}."
            )
        elif drop_count == 0:
            verb = "is" if len(resource_ids) == 1 else "are"
            print(
                f"  The {self.display_name}: {self._print_ids_or_length(resource_ids)} {verb} empty, "
                f"thus no {self.item_name} will be {'touched' if dry_run else 'dropped'}."
            )
        else:
            # Count is not supported
            print(
                f" {prefix} all {self.item_name} from {self.display_name}: "
                f"{self._print_ids_or_length(resource_ids)}."
            )


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

    def deploy_resources(
        self,
        path: Path,
        ToolGlobals: CDFToolConfig,
        dry_run: bool = False,
        has_done_drop: bool = False,
        has_dropped_data: bool = False,
        verbose: bool = False,
    ) -> UploadDeployResult | None:
        filepaths = self.find_files(path)

        prefix = "Would upload" if dry_run else "Uploading"
        print(f"[bold]{prefix} {len(filepaths)} data {self.display_name} files to CDF...[/]")
        datapoints = 0
        for filepath in filepaths:
            try:
                message, file_datapoints = self.upload(filepath, ToolGlobals, dry_run)
            except Exception as e:
                print(f"  [bold red]Error:[/] Failed to upload {filepath.name}. Error: {e!r}.")
                print(Panel(traceback.format_exc()))
                ToolGlobals.failed = True
                return None
            if verbose:
                print(message)
            datapoints += file_datapoints
        if datapoints != 0:
            return DatapointDeployResult(
                self.display_name, points=datapoints, uploaded=len(filepaths), item_name=self.item_name
            )
        else:
            return UploadDeployResult(self.display_name, uploaded=len(filepaths), item_name=self.item_name)
