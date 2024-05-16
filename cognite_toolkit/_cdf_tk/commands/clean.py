from graphlib import TopologicalSorter
from pathlib import Path

import typer
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitCleanResourceError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.load import (
    LOADER_BY_FOLDER_NAME,
    AuthLoader,
    DataSetsLoader,
    DeployResults,
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.templates import (
    BUILD_ENVIRONMENT_FILE,
)
from cognite_toolkit._cdf_tk.templates.data_classes import (
    BuildEnvironment,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    ToolkitDependenciesIncludedWarning,
    ToolkitNotSupportedWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    read_yaml_file,
)


class CleanCommand(ToolkitCommand):
    def execute(
        self, ctx: typer.Context, build_dir: str, build_env_name: str, dry_run: bool, include: list[str]
    ) -> None:
        ToolGlobals = CDFToolConfig.from_context(ctx)

        build_ = BuildEnvironment.load(
            read_yaml_file(Path(build_dir) / BUILD_ENVIRONMENT_FILE), build_env_name, "clean"
        )
        build_.set_environment_variables()

        Panel(f"[bold]Cleaning environment {build_env_name} based on config files from {build_dir}...[/]")
        build_path = Path(build_dir)
        if not build_path.is_dir():
            raise ToolkitNotADirectoryError(f"'{build_dir}'. Did you forget to run `cdf-tk build` first?")

        # The 'auth' loader is excluded, as it is run at the end.
        selected_loaders = {
            loader_cls: loader_cls.dependencies
            for folder_name, loader_classes in LOADER_BY_FOLDER_NAME.items()
            if folder_name in include and folder_name != "auth" and (build_path / folder_name).is_dir()
            for loader_cls in loader_classes
        }

        print(ToolGlobals.as_string())
        if ToolGlobals.failed:
            raise ToolkitCleanResourceError("Failure to delete data models as expected.")

        results = DeployResults([], "clean", dry_run=dry_run)
        resolved_list = list(TopologicalSorter(selected_loaders).static_order())
        if len(resolved_list) > len(selected_loaders):
            dependencies = [item.folder_name for item in resolved_list if item not in selected_loaders]
            self.warn(ToolkitDependenciesIncludedWarning(dependencies=dependencies))
        for loader_cls in reversed(resolved_list):
            if not issubclass(loader_cls, ResourceLoader):
                continue
            loader = loader_cls.create_loader(ToolGlobals)
            if type(loader) is DataSetsLoader:
                self.warn(ToolkitNotSupportedWarning(feature="Dataset clean."))
                continue
            result = loader.clean_resources(
                build_path / loader_cls.folder_name,
                ToolGlobals,
                drop=True,
                dry_run=dry_run,
                drop_data=True,
                verbose=ctx.obj.verbose,
            )
            if result:
                results[result.name] = result
            if ToolGlobals.failed:
                if results and results.has_counts:
                    print(results.counts_table())
                if results and results.has_uploads:
                    print(results.uploads_table())
                raise ToolkitCleanResourceError(f"Failure to clean {loader_cls.folder_name} as expected.")

        if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
            result = AuthLoader.create_loader(ToolGlobals, target_scopes="all").clean_resources(
                directory,
                ToolGlobals,
                drop=True,
                dry_run=dry_run,
                verbose=ctx.obj.verbose,
            )
            if ToolGlobals.failed:
                raise ToolkitCleanResourceError("Failure to clean auth as expected.")
            if result:
                results[result.name] = result
        if results.has_counts:
            print(results.counts_table())
        if results.has_uploads:
            print(results.uploads_table())
        if ToolGlobals.failed:
            raise ToolkitCleanResourceError("Failure to clean auth as expected.")
