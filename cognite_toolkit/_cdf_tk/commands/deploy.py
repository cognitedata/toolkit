from graphlib import TopologicalSorter
from pathlib import Path

import typer
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
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.templates import (
    BUILD_ENVIRONMENT_FILE,
)
from cognite_toolkit._cdf_tk.templates.data_classes import (
    BuildEnvironment,
)
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
            print("[bold yellow]WARNING:[/] Some resources were added due to dependencies.")
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
