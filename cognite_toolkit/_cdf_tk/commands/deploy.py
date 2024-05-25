import re
import traceback
from graphlib import TopologicalSorter
from pathlib import Path
from typing import cast

import typer
from cognite.client.data_classes._base import T_CogniteResourceList
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError, CogniteDuplicatedError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitCleanResourceError,
    ToolkitDeployResourceError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.load import (
    LOADER_BY_FOLDER_NAME,
    AuthLoader,
    DeployResults,
    ResourceLoader, ResourceContainerLoader,
)
from cognite_toolkit._cdf_tk.load._base_loaders import T_ID
from cognite_toolkit._cdf_tk.load.data_classes import ResourceDeployResult, ResourceContainerDeployResult
from cognite_toolkit._cdf_tk.templates import (
    BUILD_ENVIRONMENT_FILE,
)
from cognite_toolkit._cdf_tk.templates.data_classes import (
    BuildEnvironment,
)
from cognite_toolkit._cdf_tk.tk_warnings.other import ToolkitDependenciesIncludedWarning
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    read_yaml_file,
)


class DeployCommand(ToolkitCommand):
    def execute(
        self,
        ctx: typer.Context,
        build_dir: str,
        build_env_name: str,
        dry_run: bool,
        drop: bool,
        drop_data: bool,
        include: list[str],
    ) -> None:
        # Override cluster and project from the options/env variables
        ToolGlobals = CDFToolConfig.from_context(ctx)

        build_ = BuildEnvironment.load(
            read_yaml_file(Path(build_dir) / BUILD_ENVIRONMENT_FILE), build_env_name, "deploy"
        )
        build_.set_environment_variables()

        print(Panel(f"[bold]Deploying config files from {build_dir} to environment {build_env_name}...[/]"))
        build_path = Path(build_dir)
        if not build_path.is_dir():
            raise ToolkitNotADirectoryError(f"'{build_dir}'. Did you forget to run `cdf-tk build` first?")

        if not _RUNNING_IN_BROWSER:
            print(ToolGlobals.as_string())

        # The 'auth' loader is excluded, as it is run twice,
        # once with all_scoped_only and once with resource_scoped_only
        selected_loaders = {
            loader_cls: loader_cls.dependencies
            for folder_name, loader_classes in LOADER_BY_FOLDER_NAME.items()
            if folder_name in include and folder_name != "auth" and (build_path / folder_name).is_dir()
            for loader_cls in loader_classes
        }
        results = DeployResults([], "deploy", dry_run=dry_run)
        ordered_loaders = list(TopologicalSorter(selected_loaders).static_order())
        if len(ordered_loaders) > len(selected_loaders):
            dependencies = [item.folder_name for item in ordered_loaders if item not in selected_loaders]
            self.warn(ToolkitDependenciesIncludedWarning(dependencies))
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
                loader = loader_cls.create_loader(ToolGlobals)
                result = loader.clean_resources(
                    build_path / loader_cls.folder_name,
                    ToolGlobals,
                    drop=drop,
                    dry_run=dry_run,
                    drop_data=drop_data,
                    verbose=ctx.obj.verbose,
                )
                if result:
                    results[result.name] = result
                if ToolGlobals.failed:
                    raise ToolkitCleanResourceError(f"Failure to clean {loader_cls.folder_name} as expected.")

            if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
                result = AuthLoader.create_loader(ToolGlobals, target_scopes="all").clean_resources(
                    directory,
                    ToolGlobals,
                    drop=drop,
                    dry_run=dry_run,
                    verbose=ctx.obj.verbose,
                )
                if result:
                    results[result.name] = result
                if ToolGlobals.failed:
                    # TODO: Clean auth? What does that mean?
                    raise ToolkitCleanResourceError("Failure to clean auth as expected.")

            print("[bold]...cleaning complete![/]")

        arguments = dict(
            ToolGlobals=ToolGlobals,
            dry_run=dry_run,
            has_done_drop=drop,
            has_dropped_data=drop_data,
            verbose=ctx.obj.verbose,
        )
        if drop or drop_data:
            print(Panel("[bold]DEPLOYING resources...[/]"))
        if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
            # First, we need to get all the generic access, so we can create the rest of the resources.
            result = (
                AuthLoader
                .create_loader(ToolGlobals, target_scopes="all_scoped_only")
                .deploy_resources(directory, **arguments)
            )  # fmt: skip
            if ToolGlobals.failed:
                raise ToolkitDeployResourceError("Failure to deploy auth (groups) with ALL scope as expected.")
            if result:
                results[result.name] = result
            if ctx.obj.verbose:
                print("")  # Extra newline

        for loader_cls in ordered_loaders:
            result = loader_cls.create_loader(ToolGlobals).deploy_resources(  # type: ignore[assignment]
                build_path / loader_cls.folder_name, **arguments
            )
            if ToolGlobals.failed:
                if results and results.has_counts:
                    print(results.counts_table())
                if results and results.has_uploads:
                    print(results.uploads_table())
                raise ToolkitDeployResourceError(f"Failure to load/deploy {loader_cls.folder_name} as expected.")
            if result:
                results[result.name] = result
            if ctx.obj.verbose:
                print("")  # Extra newline

        if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
            # Last, we create the Groups again, but this time we do not filter out any capabilities
            # and we do not skip validation as the resources should now have been created.
            loader = AuthLoader.create_loader(ToolGlobals, target_scopes="resource_scoped_only")
            result = loader.deploy_resources(directory, **arguments)
            if ToolGlobals.failed:
                raise ToolkitDeployResourceError("Failure to deploy auth (groups) scoped to resources as expected.")
            if result:
                results[result.name] = result
        if results.has_counts:
            print(results.counts_table())
        if results.has_uploads:
            print(results.uploads_table())
        if ToolGlobals.failed:
            raise ToolkitDeployResourceError("Failure to deploy auth (groups) scoped to resources as expected.")

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
                    f"[bold red]ERROR:[/] Please compare with the API specification at {self.doc_url()}."
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
