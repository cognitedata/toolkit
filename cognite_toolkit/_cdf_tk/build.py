from __future__ import annotations

import datetime
import io
import re
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import typer
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitDuplicatedModuleError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.templates._utils import iterate_modules
from cognite_toolkit._cdf_tk.templates.data_classes import (
    BuildConfigYAML,
    SystemYAML,
)
from cognite_toolkit._cdf_tk.validation import (
    validate_modules_variables,
)

from ._commands import ToolkitCommand
from .load import FunctionLoader
from .templates._constants import PROC_TMPL_VARS_SUFFIX
from .templates._templates import (
    create_file_name,
    create_local_config,
    process_files_directory,
    process_function_directory,
    replace_variables,
    resource_folder_from_path,
    split_config,
    validate,
)
from .user_warnings import HighSeverityWarning, LowSeverityWarning


class BuildCommand(ToolkitCommand):
    def execute(
        self, ctx: typer.Context, source_path: Path, build_dir: Path, build_env_name: str, no_clean: bool
    ) -> None:
        if not source_path.is_dir():
            raise ToolkitNotADirectoryError(str(source_path))

        system_config = SystemYAML.load_from_directory(source_path, build_env_name, self.warn)
        config = BuildConfigYAML.load_from_directory(source_path, build_env_name, self.warn)
        print(
            Panel(
                f"[bold]Building config files from templates into {build_dir!s} for environment {build_env_name} using {source_path!s} as sources...[/bold]"
                f"\n[bold]Config file:[/] '{config.filepath.absolute()!s}'"
            )
        )
        config.set_environment_variables()

        self.build_config(
            build_dir=build_dir,
            source_dir=source_path,
            config=config,
            system_config=system_config,
            clean=not no_clean,
            verbose=ctx.obj.verbose,
        )

    def build_config(
        self,
        build_dir: Path,
        source_dir: Path,
        config: BuildConfigYAML,
        system_config: SystemYAML,
        clean: bool = False,
        verbose: bool = False,
    ) -> dict[Path, Path]:
        is_populated = build_dir.exists() and any(build_dir.iterdir())
        if is_populated and clean:
            shutil.rmtree(build_dir)
            build_dir.mkdir()
            if not _RUNNING_IN_BROWSER:
                print(f"  [bold green]INFO:[/] Cleaned existing build directory {build_dir!s}.")
        elif is_populated and not _RUNNING_IN_BROWSER:
            self.warn(
                LowSeverityWarning("Build directory is not empty. Run without --no-clean to remove existing files.")
            )
        elif build_dir.exists() and not _RUNNING_IN_BROWSER:
            print("  [bold green]INFO:[/] Build directory does already exist and is empty. No need to create it.")
        else:
            build_dir.mkdir(exist_ok=True)

        config.validate_environment()

        module_parts_by_name: dict[str, list[tuple[str, ...]]] = defaultdict(list)
        available_modules: set[str | tuple[str, ...]] = set()
        for module, _ in iterate_modules(source_dir):
            available_modules.add(module.name)
            module_parts = module.relative_to(source_dir).parts
            for i in range(1, len(module_parts) + 1):
                available_modules.add(module_parts[:i])

            module_parts_by_name[module.name].append(module.relative_to(source_dir).parts)

        if duplicate_modules := {
            module_name: paths
            for module_name, paths in module_parts_by_name.items()
            if len(paths) > 1 and module_name in config.environment.selected_modules_and_packages
        }:
            raise ToolkitDuplicatedModuleError(
                f"Ambiguous module selected in config.{config.environment.name}.yaml:", duplicate_modules
            )
        system_config.validate_modules(available_modules, config.environment.selected_modules_and_packages)

        selected_modules = config.get_selected_modules(system_config.packages, available_modules, verbose)

        warnings = validate_modules_variables(config.variables, config.filepath)
        if warnings:
            self.warn(LowSeverityWarning(f"Found the following warnings in config.{config.environment.name}.yaml:"))
            for warning in warnings:
                print(f"    {warning.get_message()}")

        source_by_build_path = self.process_config_files(source_dir, selected_modules, build_dir, config, verbose)

        build_environment = config.create_build_environment()
        build_environment.dump_to_file(build_dir)
        if not _RUNNING_IN_BROWSER:
            print(f"  [bold green]INFO:[/] Build complete. Files are located in {build_dir!s}/")
        return source_by_build_path

    def process_config_files(
        self,
        project_config_dir: Path,
        selected_modules: list[str | tuple[str, ...]],
        build_dir: Path,
        config: BuildConfigYAML,
        verbose: bool = False,
    ) -> dict[Path, Path]:
        source_by_build_path: dict[Path, Path] = {}
        printed_function_warning = False
        configs = split_config(config.variables)
        modules_by_variables = defaultdict(list)
        for module_path, variables in configs.items():
            for variable in variables:
                modules_by_variables[variable].append(module_path)
        number_by_resource_type: dict[str, int] = defaultdict(int)

        for module_dir, filepaths in iterate_modules(project_config_dir):
            module_parts = module_dir.relative_to(project_config_dir).parts
            is_in_selected_modules = module_dir.name in selected_modules or module_parts in selected_modules
            is_parent_in_selected_modules = any(
                parent in selected_modules for parent in (module_parts[:i] for i in range(1, len(module_parts)))
            )
            if not is_in_selected_modules and not is_parent_in_selected_modules:
                continue
            if verbose:
                print(f"  [bold green]INFO:[/] Processing module {module_dir.name}")
            local_config = create_local_config(configs, module_dir)

            # Sort to support 1., 2. etc prefixes
            def sort_key(p: Path) -> int:
                if result := re.findall(r"^(\d+)", p.stem):
                    return int(result[0])
                else:
                    return len(filepaths)

            # The builder of a module can control the order that resources are deployed by prefixing a number
            # The custom key 'sort_key' is to get the sort on integer and not the string.
            filepaths = sorted(filepaths, key=sort_key)

            @dataclass
            class ResourceFiles:
                resource_files: list[Path] = field(default_factory=list)
                other_files: list[Path] = field(default_factory=list)

            # Initialise for auth, other resource folders will be added as they are found
            files_by_resource_folder: dict[str, ResourceFiles] = defaultdict(ResourceFiles)
            for filepath in filepaths:
                try:
                    resource_folder = resource_folder_from_path(filepath)
                except ValueError:
                    if verbose:
                        print(
                            f"      [bold green]INFO:[/] The file {filepath.name} is not in a resource directory, skipping it..."
                        )
                    continue
                if filepath.suffix.lower() in PROC_TMPL_VARS_SUFFIX:
                    files_by_resource_folder[resource_folder].resource_files.append(filepath)
                else:
                    files_by_resource_folder[resource_folder].other_files.append(filepath)

            for resource_folder in files_by_resource_folder:
                for filepath in files_by_resource_folder[resource_folder].resource_files:
                    # We only want to process the yaml files for functions as the function code is handled separately.
                    if resource_folder == "functions" and filepath.suffix.lower() != ".yaml":
                        continue
                    if verbose:
                        print(f"    [bold green]INFO:[/] Processing {filepath.name}")
                    content = filepath.read_text()
                    content = replace_variables(content, local_config)
                    filename = create_file_name(filepath, number_by_resource_type)
                    destination = build_dir / resource_folder / filename
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_text(content)
                    validate(content, destination, filepath, modules_by_variables, verbose)
                    source_by_build_path[destination] = filepath

                    # If we have a function definition, we want to process the directory.
                    if (
                        resource_folder == "functions"
                        and filepath.suffix.lower() == ".yaml"
                        and re.match(FunctionLoader.filename_pattern, filepath.stem)
                    ):
                        if not printed_function_warning and sys.version_info >= (3, 12):
                            self.warn(
                                HighSeverityWarning(
                                    "The functions API does not support Python 3.12. "
                                    "It is recommended that you use Python 3.11 or 3.10 to develop functions locally."
                                )
                            )
                            printed_function_warning = True
                        process_function_directory(
                            yaml_source_path=filepath,
                            yaml_dest_path=destination,
                            module_dir=module_dir,
                            build_dir=build_dir,
                            verbose=verbose,
                        )
                        files_by_resource_folder[resource_folder].other_files = []
                    if resource_folder == "files":
                        process_files_directory(
                            files=files_by_resource_folder[resource_folder].other_files,
                            yaml_dest_path=destination,
                            module_dir=module_dir,
                            build_dir=build_dir,
                            verbose=verbose,
                        )
                        files_by_resource_folder[resource_folder].other_files = []

                if resource_folder == "timeseries_datapoints":
                    # Process all csv files
                    for filepath in files_by_resource_folder["timeseries_datapoints"].other_files:
                        if filepath.suffix.lower() != ".csv":
                            continue
                        # Special case for timeseries datapoints, we want to timeshift datapoints
                        # if the file is a csv file and we have been instructed to.
                        # The replacement is used to ensure that we read exactly the same file on Windows and Linux
                        file_content = filepath.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
                        data = pd.read_csv(io.StringIO(file_content), parse_dates=True, index_col=0)
                        destination = build_dir / resource_folder / filename
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        if "timeshift_" in data.index.name:
                            print(
                                "      [bold green]INFO:[/] Found 'timeshift_' in index name, timeshifting datapoints up to today..."
                            )
                            data.index.name = data.index.name.replace("timeshift_", "")
                            data.index = pd.DatetimeIndex(data.index)
                            periods = datetime.datetime.today() - data.index[-1]
                            data.index = pd.DatetimeIndex.shift(data.index, periods=periods.days, freq="D")
                        destination.write_text(data.to_csv())
                for filepath in files_by_resource_folder[resource_folder].other_files:
                    if verbose:
                        print(f"    [bold green]INFO:[/] Found unrecognized file {filepath}. Copying in untouched...")
                    # Copy the file as is, not variable replacement
                    destination = build_dir / filepath.parent.name / filepath.name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(filepath, destination)

        return source_by_build_path
