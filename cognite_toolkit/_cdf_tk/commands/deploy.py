from __future__ import annotations

from collections.abc import Hashable
from graphlib import TopologicalSorter
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError
from cognite.client.utils._identifier import T_ID
from rich import print
from rich.markup import escape
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.clean import CleanCommand
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    BUILD_ENVIRONMENT_FILE,
    HINT_LEAD_TEXT,
)
from cognite_toolkit._cdf_tk.data_classes import (
    BuildEnvironment,
    DatapointDeployResult,
    DeployResult,
    DeployResults,
    ResourceContainerDeployResult,
    ResourceDeployResult,
    UploadDeployResult,
)
from cognite_toolkit._cdf_tk.data_classes._module_directories import ReadModule
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceCreationError,
    ResourceUpdateError,
    ToolkitDeployResourceError,
    ToolkitFileNotFoundError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.loaders import (
    DataLoader,
    GroupLoader,
    Loader,
    RawDatabaseLoader,
    ResourceContainerLoader,
    ResourceLoader,
    ResourceWorker,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning
from cognite_toolkit._cdf_tk.tk_warnings.base import catch_warnings
from cognite_toolkit._cdf_tk.tk_warnings.other import (
    LowSeverityWarning,
    ToolkitDependenciesIncludedWarning,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection, read_yaml_file
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

from ._utils import _print_ids_or_length


class DeployCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self._clean_command = CleanCommand(print_warning, skip_tracking=True)

    def execute(
        self,
        env_vars: EnvironmentVariables,
        build_dir: Path,
        build_env_name: str | None,
        dry_run: bool,
        drop: bool,
        drop_data: bool,
        force_update: bool,
        include: list[str] | None,
        verbose: bool,
    ) -> None:
        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(
                "The build directory does not exists. Did you forget to run `cdf build` first?"
            )
        include = self._clean_command._process_include(include)
        build_environment_file_path = build_dir / BUILD_ENVIRONMENT_FILE
        if not build_environment_file_path.is_file():
            raise ToolkitFileNotFoundError(
                f"Could not find build environment file '{BUILD_ENVIRONMENT_FILE}' in '{build_dir}'. "
                "Did you forget to run `cdf build` first?"
            )

        deploy_state = BuildEnvironment.load(read_yaml_file(build_environment_file_path), build_env_name, "deploy")

        deploy_state.set_environment_variables()

        errors = deploy_state.check_source_files_changed()
        for error in errors:
            self.warn(error)
        if errors:
            raise ToolkitDeployResourceError(
                "One or more source files have been modified since the last build. Please rebuild the project."
            )
        client = env_vars.get_client(deploy_state.is_strict_validation)
        environment_vars = ""
        if not _RUNNING_IN_BROWSER:
            environment_vars = f"\n\nConnected to {env_vars.as_string()}"

        verb = "Checking" if dry_run else "Deploying"

        print(
            Panel(
                f"[bold]{verb}[/]resource files from {build_dir} directory.{environment_vars}",
                expand=False,
            )
        )

        selected_loaders = self._clean_command.get_selected_loaders(
            build_dir, deploy_state.read_resource_folders, include
        )

        results = DeployResults([], "deploy", dry_run=dry_run)

        ordered_loaders: list[type[Loader]] = []
        should_include: list[type[Loader]] = []
        # The topological sort can include loaders that are not selected, so we need to check for that.
        for loader_cls in TopologicalSorter(selected_loaders).static_order():
            if loader_cls in selected_loaders:
                ordered_loaders.append(loader_cls)
            elif (build_dir / loader_cls.folder_name).is_dir():
                should_include.append(loader_cls)
            # Otherwise, it is not in the build directory and not selected, so we skip it.
            # There should be a warning in the build step if it is missing.
        if should_include:
            self.warn(ToolkitDependenciesIncludedWarning(list({item.folder_name for item in should_include})))

        result: DeployResult | None
        if drop or drop_data:
            # Drop has to be done in the reverse order of deploy.
            if drop and drop_data:
                print(Panel("[bold] Cleaning resources as --drop and --drop-data are passed[/]"))
            elif drop:
                print(Panel("[bold] Cleaning resources as --drop is passed[/]"))
            elif drop_data:
                print(Panel("[bold] Cleaning resources as --drop-data is passed[/]"))

            for loader_cls in reversed(ordered_loaders):
                if not issubclass(loader_cls, ResourceLoader):
                    continue
                loader: ResourceLoader = loader_cls.create_loader(client, build_dir)
                result = self._clean_command.clean_resources(
                    loader,
                    env_vars=env_vars,
                    read_modules=deploy_state.read_modules,
                    drop=drop,
                    dry_run=dry_run,
                    drop_data=drop_data,
                    verbose=verbose,
                )
                if result:
                    results[result.name] = result
            print("[bold]...cleaning complete![/]")

        if drop or drop_data:
            print(Panel("[bold]DEPLOYING resources...[/]"))

        for loader_cls in ordered_loaders:
            loader_instance = loader_cls.create_loader(client, build_dir)
            result = self.deploy_resources(
                loader_instance,
                env_vars=env_vars,
                state=deploy_state,
                dry_run=dry_run,
                has_done_drop=drop,
                has_dropped_data=drop_data,
                force_update=force_update,
                verbose=verbose,
            )
            if result:
                results[result.name] = result
            if verbose:
                print("")  # Extra newline

        if results.has_counts:
            print(results.counts_table())
        if results.has_uploads:
            print(results.uploads_table())

    def deploy_resources(
        self,
        loader: Loader,
        env_vars: EnvironmentVariables,
        state: BuildEnvironment,
        dry_run: bool = False,
        has_done_drop: bool = False,
        has_dropped_data: bool = False,
        force_update: bool = False,
        verbose: bool = False,
    ) -> DeployResult | None:
        if isinstance(loader, ResourceLoader):
            return self._deploy_resources(
                loader, env_vars, state.read_modules, dry_run, has_done_drop, has_dropped_data, force_update, verbose
            )
        elif isinstance(loader, DataLoader):
            return self._deploy_data(loader, state, dry_run, verbose)
        else:
            raise ValueError(f"Unsupported loader type {type(loader)}.")

    def _deploy_resources(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        env_vars: EnvironmentVariables,
        read_modules: list[ReadModule],
        dry_run: bool = False,
        has_done_drop: bool = False,
        has_dropped_data: bool = False,
        force_update: bool = False,
        verbose: bool = False,
    ) -> ResourceDeployResult | None:
        worker = ResourceWorker(loader)
        files = worker.load_files(read_modules=read_modules)
        if not files:
            return None

        with catch_warnings(EnvironmentVariableMissingWarning) as warning_list:
            to_create, to_update, to_delete, unchanged, duplicated = worker.load_resources(
                files,
                environment_variables=env_vars.dump(include_os=True),
                is_dry_run=dry_run,
                force_update=force_update,
                verbose=verbose,
            )
        if warning_list:
            print(str(warning_list))
            self.warning_list.extend(warning_list)

        # We are not counting to_delete as these are captured by to_create.
        # (to_delete is used for resources that does not support update and instead needs to be deleted and recreated)
        nr_of_items = len(to_create) + len(to_update) + len(unchanged)
        if nr_of_items == 0:
            return ResourceDeployResult(name=loader.display_name)

        prefix = "Would deploy" if dry_run else "Deploying"
        print(f"[bold]{prefix} {nr_of_items} {loader.display_name} to CDF...[/]")
        # Moved here to avoid printing before the above message.
        if not isinstance(loader, RawDatabaseLoader):
            for duplicate in duplicated:
                self.warn(LowSeverityWarning(f"Skipping duplicate {loader.display_name} {duplicate}."))

        nr_of_created = nr_of_changed = nr_of_unchanged = nr_of_deleted = 0

        if dry_run:
            if (
                loader.support_drop
                and has_done_drop
                and (not isinstance(loader, ResourceContainerLoader) or has_dropped_data)
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
            nr_of_deleted += len(to_delete)
            if isinstance(loader, GroupLoader):
                nr_of_deleted += len(to_update)
                nr_of_created += len(to_update)
            else:
                nr_of_changed += len(to_update)
        else:
            environment_variable_warning_by_id = {
                identifier: warning
                for warning in warning_list
                if isinstance(warning, EnvironmentVariableMissingWarning)
                for identifier in warning.identifiers or []
            }
            nr_of_unchanged += len(unchanged)

            if to_delete:
                deleted = loader.delete(to_delete)
                nr_of_deleted += deleted

            if to_create:
                created = self._create_resources(to_create, loader, environment_variable_warning_by_id)
                nr_of_created += created

            if to_update:
                updated = self._update_resources(to_update, loader, environment_variable_warning_by_id)
                if isinstance(loader, GroupLoader):
                    nr_of_deleted += updated
                    nr_of_created += updated
                else:
                    nr_of_changed += updated

        if verbose:
            self._verbose_print(to_create, to_update, unchanged, loader, dry_run)

        if isinstance(loader, ResourceContainerLoader):
            return ResourceContainerDeployResult(
                name=loader.display_name,
                created=nr_of_created,
                changed=nr_of_changed,
                deleted=nr_of_deleted,
                unchanged=nr_of_unchanged,
                total=nr_of_items,
                item_name=loader.item_name,
            )
        else:
            return ResourceDeployResult(
                name=loader.display_name,
                created=nr_of_created,
                changed=nr_of_changed,
                deleted=nr_of_deleted,
                unchanged=nr_of_unchanged,
                total=nr_of_items,
            )

    def _verbose_print(
        self,
        to_create: T_CogniteResourceList,
        to_update: T_CogniteResourceList,
        unchanged: T_CogniteResourceList,
        loader: ResourceLoader,
        dry_run: bool,
    ) -> None:
        print_outs = []
        prefix = "Would have " if dry_run else ""
        if to_create:
            print_outs.append(f"{prefix}Created {_print_ids_or_length(loader.get_ids(to_create), limit=20)}")
        if to_update:
            print_outs.append(f"{prefix}Updated {_print_ids_or_length(loader.get_ids(to_update), limit=20)}")
        if unchanged:
            print_outs.append(
                f"{'Untouched' if dry_run else 'Unchanged'} {_print_ids_or_length(loader.get_ids(unchanged), limit=5)}"
            )
        prefix_message = f" {loader.display_name}: "
        if len(print_outs) == 1:
            print(f"{prefix_message}{print_outs[0]}")
        elif len(print_outs) == 2:
            print(f"{prefix_message}{print_outs[0]} and {print_outs[1]}")
        else:
            print(f"{prefix_message}{', '.join(print_outs[:-1])} and {print_outs[-1]}")

    def _create_resources(
        self,
        resources: T_CogniteResourceList,
        loader: ResourceLoader,
        environment_variable_warning_by_id: dict[Hashable, EnvironmentVariableMissingWarning],
    ) -> int:
        try:
            created = loader.create(resources)
        except CogniteAPIError as e:
            message = f"Failed to create resource(s). Error: {escape(str(e))!s}."
            if hint := self._environment_variable_hint(loader.get_ids(resources), environment_variable_warning_by_id):
                message += hint
            raise ResourceCreationError(message) from e
        except CogniteDuplicatedError as e:
            self.warn(
                LowSeverityWarning(
                    f"{len(e.duplicated)} out of {len(resources)} resource(s) already exist(s). {len(e.successful or [])} resource(s) created."
                )
            )
        else:
            return len(created) if created is not None else 0
        return 0

    def _update_resources(
        self,
        resources: T_CogniteResourceList,
        loader: ResourceLoader,
        environment_variable_warning_by_id: dict[Hashable, EnvironmentVariableMissingWarning],
    ) -> int:
        try:
            updated = loader.update(resources)
        except CogniteAPIError as e:
            message = f"Failed to update resource(s). Error: {escape(str(e))}."
            if hint := self._environment_variable_hint(loader.get_ids(resources), environment_variable_warning_by_id):
                message += hint
            raise ResourceUpdateError(message) from e

        return len(updated)

    @staticmethod
    def _environment_variable_hint(
        identifiers: list[Hashable],
        environment_variable_warning_by_id: dict[Hashable, EnvironmentVariableMissingWarning],
    ) -> str:
        if not environment_variable_warning_by_id:
            return ""
        missing_variables: set[str] = set()
        for identifier in identifiers:
            if warning := environment_variable_warning_by_id.get(identifier):
                missing_variables.update(warning.variables)
        if missing_variables:
            variables_str = humanize_collection(missing_variables)
            suffix = "s" if len(missing_variables) > 1 else ""
            return f"\n  {HINT_LEAD_TEXT}This is likely due to missing environment variable{suffix}: {variables_str}"
        return ""

    def _deploy_data(
        self,
        loader: DataLoader,
        state: BuildEnvironment,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> UploadDeployResult:
        prefix = "Would upload" if dry_run else "Uploading"
        print(f"[bold]{prefix} {loader.display_name} files to CDF...[/]")

        datapoints = 0
        file_counts = 0
        for message, file_datapoints in loader.upload(state, dry_run):
            if verbose:
                print(message)
            datapoints += file_datapoints
            file_counts += 1

        if datapoints != 0:
            return DatapointDeployResult(
                loader.display_name, points=datapoints, uploaded=file_counts, item_name=loader.item_name
            )
        else:
            return UploadDeployResult(loader.display_name, uploaded=file_counts, item_name=loader.item_name)
