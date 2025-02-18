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
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    BUILD_ENVIRONMENT_FILE,
    HINT_LEAD_TEXT,
    HINT_LEAD_TEXT_LEN,
)
from cognite_toolkit._cdf_tk.data_classes import (
    BuildEnvironment,
    DeployResults,
    ResourceContainerDeployResult,
    ResourceDeployResult,
)
from cognite_toolkit._cdf_tk.data_classes._module_directories import ReadModule
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitCleanResourceError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    DataLoader,
    DataSetsLoader,
    RawDatabaseLoader,
    ResourceContainerLoader,
    ResourceLoader,
    ResourceWorker,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, Loader, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.tk_warnings import (
    LowSeverityWarning,
    MediumSeverityWarning,
    ToolkitDependenciesIncludedWarning,
    ToolkitNotSupportedWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    humanize_collection,
    read_yaml_file,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

from ._utils import _print_ids_or_length

AVAILABLE_DATA_TYPES: tuple[str, ...] = tuple(LOADER_BY_FOLDER_NAME)


class CleanCommand(ToolkitCommand):
    def clean_resources(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        env_vars: EnvironmentVariables,
        read_modules: list[ReadModule],
        dry_run: bool = False,
        drop: bool = True,
        drop_data: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult | None:
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

        worker = ResourceWorker(loader)
        files = worker.load_files(read_modules=read_modules)
        if not files:
            return None
        # Since we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
        existing_resources, duplicated = worker.load_resources(
            filepaths=files,
            return_existing=True,
            environment_variables=env_vars.dump(include_os=True),
            is_dry_run=True,
            verbose=verbose,
        )
        nr_of_existing = len(existing_resources)

        if drop:
            prefix = "Would clean" if dry_run else "Cleaning"
            with_data = "with data " if isinstance(loader, ResourceContainerLoader) else ""
        else:
            prefix = "Would drop data from" if dry_run else "Dropping data from"
            with_data = ""
        print(f"[bold]{prefix} {nr_of_existing} {loader.display_name} {with_data}from CDF...[/]")
        if not isinstance(loader, RawDatabaseLoader):
            for duplicate in duplicated:
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
                total=nr_of_existing,
                dropped_datapoints=nr_of_dropped_datapoints,
                item_name=loader.item_name,
            )
        elif not isinstance(self, ResourceContainerLoader) and drop:
            nr_of_deleted = self._delete_resources(existing_resources, loader, dry_run, verbose)
            if verbose:
                print("")
            return ResourceDeployResult(name=loader.display_name, deleted=nr_of_deleted, total=nr_of_existing)
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
            print(f" {prefix} all {loader.item_name} from {loader.display_name}: {_print_ids_or_length(resource_ids)}.")

    def execute(
        self,
        env_vars: EnvironmentVariables,
        build_dir: Path,
        build_env_name: str | None,
        dry_run: bool,
        include: list[str] | None,
        verbose: bool,
    ) -> None:
        if not build_dir.exists():
            raise ToolkitNotADirectoryError(
                "The build directory does not exists. Did you forget to run `cdf-tk build` first?"
            )
        clean_state = BuildEnvironment.load(read_yaml_file(build_dir / BUILD_ENVIRONMENT_FILE), build_env_name, "clean")
        clean_state.set_environment_variables()
        errors = clean_state.check_source_files_changed()
        for error in errors:
            self.warn(error)
        if errors:
            raise ToolkitCleanResourceError(
                "One or more source files have been modified since the last build. Please rebuild the project."
            )
        client = env_vars.get_client(clean_state.is_strict_validation)
        environment_vars = ""
        if not _RUNNING_IN_BROWSER:
            environment_vars = f"\n\nConnected to {env_vars.as_string()}"

        action = ""
        if dry_run:
            action = "(dry-run) "

        print(
            Panel(
                f"[bold]Cleaning {action}[/]resource from CDF project {client.config.project} based "
                f"on resource files in {build_dir} directory."
                f"{environment_vars}",
                expand=False,
            )
        )

        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(f"'{build_dir}'. Did you forget to run `cdf-tk build` first?")

        selected_loaders = self.get_selected_loaders(build_dir, clean_state.read_resource_folders, include)

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
            loader = loader_cls.create_loader(client, build_dir)
            if type(loader) is DataSetsLoader:
                self.warn(ToolkitNotSupportedWarning(feature="Dataset clean."))
                continue
            result = self.clean_resources(
                loader,
                env_vars=env_vars,
                read_modules=clean_state.read_modules,
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

    def get_selected_loaders(
        self, build_dir: Path, read_resource_folders: set[str], include: list[str] | None
    ) -> dict[type[Loader], frozenset[type[Loader]]]:
        selected_loaders: dict[type[Loader], frozenset[type[Loader]]] = {}
        for folder_name, loader_classes in LOADER_BY_FOLDER_NAME.items():
            if include is not None and folder_name not in include:
                continue
            if folder_name in read_resource_folders:
                selected_loaders.update({loader_cls: loader_cls.dependencies for loader_cls in loader_classes})
                continue
            if not (build_dir / folder_name).is_dir():
                continue
            folder_has_supported_files = False
            for loader_cls in loader_classes:
                if loader_cls.any_supported_files(build_dir / folder_name):
                    folder_has_supported_files = True
                    selected_loaders[loader_cls] = loader_cls.dependencies
                elif issubclass(loader_cls, DataLoader):
                    # Data Loaders are always included, as they will have
                    # the files in the module folder and not the build folder.
                    selected_loaders[loader_cls] = loader_cls.dependencies

            if not folder_has_supported_files:
                kinds = [loader_cls.kind for loader_cls in loader_classes]
                yaml_file = next((build_dir / folder_name).glob("*.yaml"), None)
                suggestion = ""
                if yaml_file:
                    suggestion = f"\n{' ' * HINT_LEAD_TEXT_LEN}For example: '{yaml_file.stem}.{kinds[0]}.yaml'."
                self.warn(
                    MediumSeverityWarning(
                        f"No supported files found in {folder_name!r} folder. Skipping...\n"
                        f"{HINT_LEAD_TEXT}All resource in the {folder_name!r} folder are expected to have suffix: "
                        f"{humanize_collection(kinds)!r}.{suggestion}"
                    )
                )
        return selected_loaders

    @staticmethod
    def _process_include(include: list[str] | None) -> list[str]:
        if include and (invalid_types := set(include).difference(AVAILABLE_DATA_TYPES)):
            raise ToolkitValidationError(
                f"Invalid resource types specified: {invalid_types}, available types: {AVAILABLE_DATA_TYPES}"
            )
        include = include or list(AVAILABLE_DATA_TYPES)
        return include
