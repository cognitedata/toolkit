from __future__ import annotations

import traceback
from graphlib import TopologicalSorter
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import BUILD_ENVIRONMENT_FILE
from cognite_toolkit._cdf_tk.data_classes import (
    BuildEnvironment,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitCleanResourceError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    DataSetsLoader,
    DeployResults,
    ResourceContainerLoader,
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, Loader, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.loaders.data_classes import ResourceContainerDeployResult, ResourceDeployResult
from cognite_toolkit._cdf_tk.tk_warnings import (
    LowSeverityWarning,
    MediumSeverityWarning,
    ToolkitDependenciesIncludedWarning,
    ToolkitNotSupportedWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    read_yaml_file,
)

from ._utils import _print_ids_or_length, _remove_duplicates


class CleanCommand(ToolkitCommand):
    def clean_resources(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        ToolGlobals: CDFToolConfig,
        dry_run: bool = False,
        drop: bool = True,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult:
        if not isinstance(loader, ResourceContainerLoader) and not drop:
            # Skipping silently as this, we will not drop data or delete this resource
            return ResourceDeployResult(name=loader.display_name)
        if not loader.support_drop:
            print(f"  [bold green]INFO:[/] {loader.display_name!r} cleaning is not supported, skipping...")
            return ResourceDeployResult(name=loader.display_name)
        elif isinstance(loader, ResourceContainerLoader) and not drop_data:
            print(
                f"  [bold]INFO:[/] Skipping cleaning of {loader.display_name!r}. This is a data resource (it contains "
                f"data and is not only configuration/metadata) and therefore "
                "requires the --drop-data flag to be set to perform cleaning..."
            )
            return ResourceContainerDeployResult(name=loader.display_name, item_name=loader.item_name)

        filepaths = loader.find_files()

        # Since we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
        loaded_resources = self._load_files(loader, filepaths, ToolGlobals, skip_validation=True)

        # Duplicates are warned in the build step, but the use might continue, so we
        # need to check for duplicates here as well.
        loaded_resources, duplicates = _remove_duplicates(loaded_resources, loader)

        capabilities = loader.get_required_capability(loaded_resources)
        if capabilities:
            ToolGlobals.verify_authorization(capabilities, action=f"clean {loader.display_name}")

        nr_of_items = len(loaded_resources)
        if nr_of_items == 0:
            return ResourceDeployResult(name=loader.display_name)

        existing_resources = loader.retrieve(loader.get_ids(loaded_resources)).as_write()
        nr_of_existing = len(existing_resources)

        if drop:
            prefix = "Would clean" if dry_run else "Cleaning"
            with_data = "with data " if isinstance(loader, ResourceContainerLoader) else ""
        else:
            prefix = "Would drop data from" if dry_run else "Dropping data from"
            with_data = ""
        print(f"[bold]{prefix} {nr_of_existing} {loader.display_name} {with_data}from CDF...[/]")
        for duplicate in duplicates:
            self.warn(LowSeverityWarning(f"Duplicate {loader.display_name} {duplicate}."))

        # Deleting resources.
        if isinstance(loader, ResourceContainerLoader) and drop_data:
            nr_of_dropped_datapoints = self._drop_data(existing_resources, loader, dry_run, verbose)
            if drop:
                nr_of_deleted = self._delete_resources(existing_resources, loader, dry_run, verbose)
            else:
                nr_of_deleted = 0
            if verbose:
                print("")
            return ResourceContainerDeployResult(
                name=loader.display_name,
                deleted=nr_of_deleted,
                total=nr_of_items,
                dropped_datapoints=nr_of_dropped_datapoints,
                item_name=loader.item_name,
            )
        elif not isinstance(self, ResourceContainerLoader) and drop:
            nr_of_deleted = self._delete_resources(existing_resources, loader, dry_run, verbose)
            if verbose:
                print("")
            return ResourceDeployResult(name=loader.display_name, deleted=nr_of_deleted, total=nr_of_items)
        else:
            return ResourceDeployResult(name=loader.display_name)

    def _delete_resources(
        self, loaded_resources: T_CogniteResourceList, loader: ResourceLoader, dry_run: bool, verbose: bool
    ) -> int:
        nr_of_deleted = 0
        resource_ids = loader.get_ids(loaded_resources)
        if dry_run:
            nr_of_deleted += len(resource_ids)
            if verbose:
                print(f"  Would have deleted {_print_ids_or_length(resource_ids)}.")
            return nr_of_deleted

        try:
            nr_of_deleted += loader.delete(resource_ids)
        except CogniteAPIError as e:
            self.warn(MediumSeverityWarning(f"Failed to delete {_print_ids_or_length(resource_ids)}. Error {e}."))
            if verbose:
                print(Panel(traceback.format_exc()))
        except CogniteNotFoundError:
            if verbose:
                print(f"  [bold]INFO:[/] {_print_ids_or_length(resource_ids)} do(es) not exist.")
        except Exception as e:
            self.warn(MediumSeverityWarning(f"Failed to delete {_print_ids_or_length(resource_ids)}. Error {e}."))
            if verbose:
                print(Panel(traceback.format_exc()))
        else:  # Delete succeeded
            if verbose:
                print(f"  Deleted {_print_ids_or_length(resource_ids)}.")
        return nr_of_deleted

    def _drop_data(
        self, loaded_resources: T_CogniteResourceList, loader: ResourceContainerLoader, dry_run: bool, verbose: bool
    ) -> int:
        nr_of_dropped = 0
        resource_ids = loader.get_ids(loaded_resources)
        if dry_run:
            resource_drop_count = loader.count(resource_ids)
            nr_of_dropped += resource_drop_count
            if verbose:
                self._verbose_print_drop(resource_drop_count, resource_ids, loader, dry_run)
            return nr_of_dropped

        try:
            resource_drop_count = loader.drop_data(resource_ids)
            nr_of_dropped += resource_drop_count
        except CogniteAPIError as e:
            if e.code == 404 and verbose:
                print(f"  [bold]INFO:[/] {len(resource_ids)} {loader.display_name} do(es) not exist.")
        except CogniteNotFoundError:
            return nr_of_dropped
        except Exception as e:
            self.warn(
                MediumSeverityWarning(
                    f"Failed to drop {loader.item_name} from {len(resource_ids)} {loader.display_name}. Error {e}."
                )
            )
            if verbose:
                print(Panel(traceback.format_exc()))
        else:  # Delete succeeded
            if verbose:
                self._verbose_print_drop(resource_drop_count, resource_ids, loader, dry_run)
        return nr_of_dropped

    def _verbose_print_drop(
        self, drop_count: int, resource_ids: SequenceNotStr[T_ID], loader: ResourceContainerLoader, dry_run: bool
    ) -> None:
        prefix = "Would have dropped" if dry_run else "Dropped"
        if drop_count > 0:
            print(
                f"  {prefix} {drop_count:,} {loader.item_name} from {loader.display_name}: "
                f"{_print_ids_or_length(resource_ids)}."
            )
        elif drop_count == 0:
            verb = "is" if len(resource_ids) == 1 else "are"
            print(
                f"  The {loader.display_name}: {_print_ids_or_length(resource_ids)} {verb} empty, "
                f"thus no {loader.item_name} will be {'touched' if dry_run else 'dropped'}."
            )
        else:
            # Count is not supported
            print(
                f" {prefix} all {loader.item_name} from {loader.display_name}: "
                f"{_print_ids_or_length(resource_ids)}."
            )

    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        build_dir_raw: str,
        build_env_name: str,
        dry_run: bool,
        include: list[str],
        verbose: bool,
    ) -> None:
        build_dir = Path(build_dir_raw)
        if not build_dir.exists():
            raise ToolkitNotADirectoryError(
                "The build directory does not exists. Did you forget to run `cdf-tk build` first?"
            )
        build_ = BuildEnvironment.load(read_yaml_file(build_dir / BUILD_ENVIRONMENT_FILE), build_env_name, "clean")
        build_.set_environment_variables()
        errors = build_.check_source_files_changed()
        for error in errors:
            self.warn(error)
        if errors:
            raise ToolkitCleanResourceError(
                "One or more source files have been modified since the last build. " "Please rebuild the project."
            )

        Panel(f"[bold]Cleaning environment {build_env_name} based on config files from {build_dir}...[/]")

        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(f"'{build_dir}'. Did you forget to run `cdf-tk build` first?")

        # The 'auth' loader is excluded, as it is run at the end.
        selected_loaders = {
            loader_cls: loader_cls.dependencies
            for folder_name, loader_classes in LOADER_BY_FOLDER_NAME.items()
            if folder_name in include and (build_dir / folder_name).is_dir()
            for loader_cls in loader_classes
        }

        print(ToolGlobals.as_string())

        results = DeployResults([], "clean", dry_run=dry_run)

        resolved_list: list[type[Loader]] = []
        should_include: list[type[Loader]] = []
        # The topological sort can include loaders that are not selected, so we need to check for that.
        for loader_cls in TopologicalSorter(selected_loaders).static_order():
            if loader_cls in selected_loaders:
                resolved_list.append(loader_cls)
            elif (build_dir / loader_cls.folder_name).is_dir():
                should_include.append(loader_cls)
            # Otherwise, it is not in the build directory and not selected, so we skip it.
            # There should be a warning in the build step if it is missing.
        if should_include:
            self.warn(ToolkitDependenciesIncludedWarning([item.folder_name for item in should_include]))

        for loader_cls in reversed(resolved_list):
            if not issubclass(loader_cls, ResourceLoader):
                continue
            loader = loader_cls.create_loader(ToolGlobals, build_dir)
            if type(loader) is DataSetsLoader:
                self.warn(ToolkitNotSupportedWarning(feature="Dataset clean."))
                continue
            result = self.clean_resources(
                loader,
                ToolGlobals,
                drop=True,
                dry_run=dry_run,
                drop_data=True,
                verbose=verbose,
            )
            if result:
                results[result.name] = result
        if results.has_counts:
            print(results.counts_table())
        if results.has_uploads:
            print(results.uploads_table())
