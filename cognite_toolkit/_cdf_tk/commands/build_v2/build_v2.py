import os
import re
import shutil
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import zip_longest
from pathlib import Path
from typing import Any, Literal, cast

import questionary
import yaml
from pydantic import JsonValue, TypeAdapter, ValidationError
from questionary import Choice
from rich.console import Console, Group, RenderableType
from rich.progress import Progress
from rich.table import Table

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2._module_parser import ModuleParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildFolder,
    BuildLineage,
    BuildParameters,
    BuildSourceFiles,
    BuiltModule,
    ConfigYAML,
    InsightList,
    Module,
    ModuleSource,
    RelativeDirPath,
    ResourceType,
    ValidationType,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource, ValidationResult
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight, ModelSyntaxWarning
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    SUPPORTS_VARIABLE_REPLACEMENT,
    BuildSource,
    BuildVariable,
    FailedReadYAMLFile,
    IgnoredFile,
    ReadResource,
    ReadYAMLFile,
    SuccessfulReadYAMLFile,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING, HINT_LEAD_TEXT, MODULES
from cognite_toolkit._cdf_tk.data_classes._tracking_info import BuildTracking, to_tracking_key
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitNotADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_CRUD_BY_FOLDER_NAME,
    ResourceIO,
)
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, SuccessExtra
from cognite_toolkit._cdf_tk.rules import LocalRulesOrchestrator, ToolkitGlobalRuleSet, get_global_rules_registry
from cognite_toolkit._cdf_tk.rules._base import FailedValidation, RuleSetStatus
from cognite_toolkit._cdf_tk.ui import ToolkitPanel, ToolkitPanelSection, ToolkitTable
from cognite_toolkit._cdf_tk.utils import calculate_hash, humanize_collection, safe_write
from cognite_toolkit._cdf_tk.utils.file import (
    read_yaml_content,
    relative_to_if_possible,
    safe_rmtree,
    yaml_safe_dump,
)
from cognite_toolkit._cdf_tk.validation import humanize_validation_error
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource


@dataclass
class ValidationStep:
    status: RuleSetStatus
    rule: ToolkitGlobalRuleSet


SelectionSource = Literal["modules", "config", "interactive"]


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuildFolder:
        console = client.console if client else Console(markup=True)

        # Track build duration
        build_start_time = datetime.now(timezone.utc)

        self._validate_build_parameters(parameters, console, sys.argv)
        build_files = self._read_file_system(parameters)
        selection_source: SelectionSource = (
            "modules"
            if parameters.user_selected_modules
            else "config"
            if build_files.selected_modules is not None
            else "interactive"
        )

        build_source = self._find_modules(build_files)
        self._display_module_sources(
            build_source, console, parameters.verbose, selection_source, parameters.config_file_name
        )

        self._prepare_build_directory(parameters.build_dir)
        built_modules = self._build_modules(build_source.modules, parameters.build_dir, console)

        plan = self._create_validation_plan(built_modules, client)
        self._display_validation_plan(plan, console)
        validation_results = self._run_validation(plan, console)

        build_folder = BuildFolder(
            organization_dir=parameters.organization_dir.resolve(),
            build_dir=parameters.build_dir,
            built_modules=built_modules,
            validation_results=validation_results,
            all_variables=build_source.all_variables,
            started_at=build_start_time,
            finished_at=datetime.now(timezone.utc),
        )

        insights = build_folder.all_insights
        self._display_insights(insights, parameters.insight_path, console, parameters.verbose)
        self._display_build_summary(build_folder, insights, console, parameters.verbose)

        self._track_build_results(build_folder, insights, client)

        self._write_results(insights, build_folder, parameters, client.config.project if client else None)

        return build_folder

    @classmethod
    def _validate_build_parameters(cls, parameters: BuildParameters, console: Console, user_args: list[str]) -> None:
        """Checks that the user has the correct folders set up and that the config file (if provided) exists."""

        # Set up the variables
        organization_dir = parameters.organization_dir
        config_yaml_path: Path | None = parameters.config_yaml.resolve() if parameters.config_yaml else None
        module_directory = parameters.modules_directory

        # Execute the checks.
        if module_directory.exists() and (config_yaml_path is None or config_yaml_path.exists()):
            # All good.
            return

        # Display what Toolkit expects.
        if isinstance(config_yaml_path, Path):
            content = f"  ┣ {MODULES}/\n  ┗ {config_yaml_path.name}\n"
        else:
            content = f"  ┗ {MODULES}\n"
        organization_dir_display = relative_to_if_possible(organization_dir)
        expected_panel = ToolkitPanel(
            f"Toolkit expects the following structure:\n{organization_dir_display.as_posix()!r}/\n{content}",
            expand=False,
        )
        console.print(expected_panel)

        if not organization_dir.exists():
            raise ToolkitNotADirectoryError(f"Organization directory '{organization_dir_display.as_posix()}' not found")
        elif module_directory.exists() and config_yaml_path is not None and not config_yaml_path.exists():
            raise ToolkitFileNotFoundError(
                f"Config YAML file '{relative_to_if_possible(config_yaml_path).as_posix()}' not found"
            )
        elif not module_directory.exists():
            # This is likely the most common error. The user pass in Path.cwd() as the organization directory,
            # but the actual organization directory is a subdirectory.
            candidate_org = next(
                (subdir for subdir in Path.cwd().iterdir() if subdir.is_dir() and (subdir / MODULES).exists()), None
            )
            if candidate_org:
                display_path = relative_to_if_possible(candidate_org)
                suggested_command = cls._create_suggested_command(display_path, user_args)
                console.print(
                    f"\n{HINT_LEAD_TEXT}Did [red]you[/red] mean to use the command {suggested_command}?\n",
                    markup=True,
                )
                cdf_toml = CDFToml.load()
                if not cdf_toml.cdf.has_user_set_default_org:
                    console.print(
                        f"{HINT_LEAD_TEXT} You can specify a 'default_organization_dir = ...' in the 'cdf' section of your "
                        f"'{CDFToml.file_name}' file to avoid using the -o/--organization-dir argument",
                        markup=True,
                    )
            raise ToolkitNotADirectoryError(
                f"Could not find the modules directory at '{relative_to_if_possible(module_directory).as_posix()}'"
            )
        else:
            raise NotImplementedError("Unhandled case. Please report this.")

    @classmethod
    def _create_suggested_command(cls, display_path: Path, user_args: list[str]) -> str:
        suggestion = ["cdf"]
        skip_next = False
        found = False
        for arg in user_args[1:]:
            if skip_next:
                skip_next = False
                continue

            arg_name = arg.split("=")[0]
            if arg_name in ("-o", "--organization-dir"):
                suggestion.append(f"{arg_name} {display_path}")
                found = True
                if "=" not in arg:
                    skip_next = True
                continue
            suggestion.append(arg)
        if not found:
            suggestion.append(f"-o {display_path}")
        return f"'{' '.join(suggestion)}'"

    def _find_modules(self, build: BuildSourceFiles) -> BuildSource:
        source_by_module_id, orphan_files = ModuleParser.find_modules(build.yaml_files, build.organization_dir)

        if build.selected_modules is None:
            user_selected_modules = self._ask_user_to_select_modules(list(source_by_module_id.values()))
        else:
            user_selected_modules = build.selected_modules

        return ModuleParser.parse(build, user_selected_modules, source_by_module_id, orphan_files)

    @classmethod
    def _ask_user_to_select_modules(cls, available_modules: list[ModuleSource]) -> set[RelativeDirPath | str]:
        choices = [
            Choice(
                title=f"{module.name} ({module.id.as_posix()})",
                value=module.id,
            )
            for module in available_modules
        ]
        if not available_modules:
            raise ToolkitValueError("No modules found to build.")
        result = questionary.checkbox("Which modules would you like to build?", choices=choices).unsafe_ask()
        if result is None:
            raise ToolkitValueError("Build cancelled by user.")
        return set(result)

    def _display_module_sources(
        self,
        build_source: BuildSource,
        console: Console,
        verbose: bool,
        selection_source: SelectionSource,
        config_file_name: str,
    ) -> None:
        module_count = len(build_source.modules)
        total_files = build_source.total_files
        read_variables = len({variable.id for module in build_source.modules for variable in module.variables})
        resource_type_count = len(
            {
                resource_type
                for module in build_source.modules
                for resource_type in module.resource_files_by_folder.keys()
            }
        )
        ambiguous_selected_count = sum(1 for selection in build_source.ambiguous_selection if selection.is_selected)
        misplaced_modules_count = len(build_source.misplaced_modules)
        non_existing_module_count = len(build_source.non_existing_module_names)
        invalid_variable_count = len(build_source.invalid_variables)
        orphan_yaml_count = len(build_source.orphan_yaml_files)

        module_dir_display = relative_to_if_possible(build_source.module_dir)

        summary_sections: list[ToolkitPanelSection] = []
        summary_sections.append(
            ToolkitPanelSection(
                title="Directory",
                description=module_dir_display.as_posix(),
            )
        )

        if verbose:
            summary_sections.append(
                ToolkitPanelSection(
                    title="Selection",
                    description=self._module_selection_message(selection_source, config_file_name),
                    content=[f"[green] -[/] {module.id.as_posix()}" for module in build_source.modules],
                )
            )
        summary_sections.append(
            ToolkitPanelSection(
                title="Loaded",
                content=[
                    f"[green]✓[/] [bold]{module_count}[/] modules",
                    f"[green]✓[/] [bold]{total_files}[/] total resource files",
                    f"[green]✓[/] [bold]{resource_type_count}[/] resource types",
                    *([f"[green]✓[/] [bold]{read_variables}[/] read variables"] if read_variables else []),
                ],
            )
        )

        border_color = 0
        errors: list[str] = []
        issue_summary_section_content: list[str] = []
        issue_details_section_content: list[RenderableType] = []

        if ambiguous_selected_count:
            issue_summary_section_content.append(
                f"[red]✗[/] [bold]{ambiguous_selected_count}[/] user-selected modules had an ambiguous match with multiple module directories."
            )
            table = ToolkitTable(title="Ambiguous Module Selections")
            table.add_column("Module Name", style="red")
            table.add_column("Matching Paths", style="dim")
            for selection in build_source.ambiguous_selection:
                if selection.is_selected:
                    paths_str = ", ".join(p.as_posix() for p in selection.module_paths)
                    table.add_row(selection.name, paths_str)
            issue_details_section_content.append(table.as_panel_detail())
            border_color = max(border_color, 2)

        if misplaced_modules_count:
            issue_summary_section_content.append(
                f"[yellow]![/] [bold]{misplaced_modules_count}[/] modules are located directly under the another module (misplaced modules)."
            )
            table = ToolkitTable(title="Misplaced Modules")
            table.add_column("Module Path", style="red")
            table.add_column("Parent Modules", style="dim")
            for misplaced in build_source.misplaced_modules:
                parents_str = ", ".join(p.as_posix() for p in misplaced.parent_modules)
                table.add_row(misplaced.id.as_posix(), parents_str)
            issue_details_section_content.append(table.as_panel_detail())
            border_color = max(border_color, 1)
        if non_existing_module_count:
            issue_summary_section_content.append(
                f"[red]✗[/] [bold]{non_existing_module_count}[/] user-selected module names did not match any module directory (non existing module names)."
            )
            table = ToolkitTable(title="Non-Existing Module Names")
            table.add_column("Module Name", style="red")
            table.add_column("Closest Matches", style="dim")
            for non_existing in build_source.non_existing_module_names:
                matches_str = ", ".join(non_existing.closest_matches) if non_existing.closest_matches else "-"
                table.add_row(non_existing.name, matches_str)
            issue_details_section_content.append(table.as_panel_detail())
            border_color = max(border_color, 2)

        if invalid_variable_count:
            issue_summary_section_content.append(
                f"[yellow]![/] [bold]{invalid_variable_count}[/] invalid variables found across modules and config YAML (invalid variables)."
            )
            table = ToolkitTable(title="Invalid Variables")
            table.add_column("Variable Path", style="red")
            table.add_column("Error", style="dim")
            for invalid_var in build_source.invalid_variables:
                table.add_row(invalid_var.id.as_posix(), invalid_var.error.message)
            issue_details_section_content.append(table.as_panel_detail())
            border_color = max(border_color, 1)

        if orphan_yaml_count:
            issue_summary_section_content.append(
                f"[yellow]![/] [bold]{orphan_yaml_count}[/] YAML files found directly under the modules directory that are not part of any module (orphan YAML files)."
            )
            table = ToolkitTable(title="Orphan YAML Files")
            table.add_column("File Path", style="yellow")
            for orphan_file in build_source.orphan_yaml_files:
                table.add_row(orphan_file.as_posix())
            issue_details_section_content.append(table.as_panel_detail())
            border_color = max(border_color, 1)

        if issue_summary_section_content:
            if not verbose:
                issue_summary_section_content.append("[italic]Use -v or --verbose to see details[/italic]")

            summary_sections.append(ToolkitPanelSection(title="Issues detected", content=issue_summary_section_content))

        if verbose and issue_details_section_content:
            summary_sections.append(ToolkitPanelSection(title="Issue details", content=issue_details_section_content))

        border_style = {0: "green", 1: "yellow", 2: "red"}[border_color]
        console.print(
            ToolkitPanel(Group(*summary_sections), title="[bold]Loading modules[/]", border_style=border_style)
        )

        if errors:
            console.print("\n")
            raise ToolkitValueError(
                f"Cannot build {module_dir_display.as_posix()!r}. You are not allowed to have"
                f" {humanize_collection(errors)}."
            )
        return None

    @staticmethod
    def _module_selection_message(selection_source: SelectionSource, config_file_name: str) -> str:
        if selection_source == "modules":
            return "provided as arguments, overrides selection in config.env.yaml"
        if selection_source == "config":
            return f"specified in {config_file_name or 'config.env.yaml'}"
        return "interactive"

    @classmethod
    def _read_file_system(cls, parameters: BuildParameters) -> BuildSourceFiles:
        """Reads the file system to find the YAML files to build along with config.<name>.yaml if it exists."""
        selected: set[RelativeDirPath | str] | None = None
        variables: dict[str, JsonValue] = {}
        cdf_project: str = os.environ.get("CDF_PROJECT", "UNKNOWN")
        validation_type: ValidationType = "prod"
        if parameters.user_selected_modules:
            selected, errors = cls._parse_user_selection(parameters.user_selected_modules, parameters.organization_dir)
            if errors:
                raise ToolkitValueError("Invalid module selection:\n" + "\n".join(f"- {error}" for error in errors))

        if parameters.config_yaml:
            config_path = parameters.config_yaml.resolve()
            try:
                config = ConfigYAML.from_yaml_file(config_path)
            except ValidationError as e:
                errors = humanize_validation_error(e)
                raise ToolkitValueError(
                    f"Config YAML file '{config_path.as_posix()}' is invalid:\n{'- '.join(errors)}"
                ) from e
            if not parameters.user_selected_modules and config.environment.selected:
                selected, errors = cls._parse_user_selection(config.environment.selected, parameters.organization_dir)
                if errors:
                    raise ToolkitValueError("Invalid module selection:\n" + "\n".join(f"- {error}" for error in errors))
            variables = config.variables or {}
            cdf_project = config.environment.project
            validation_type = config.environment.validation_type

        yaml_files = [
            yaml_file.relative_to(parameters.organization_dir)
            for yaml_file in parameters.modules_directory.rglob("*.y*ml")
        ]
        return BuildSourceFiles(
            yaml_files=yaml_files,
            selected_modules=selected,
            variables=variables,
            validation_type=validation_type,
            cdf_project=cdf_project,
            organization_dir=parameters.organization_dir.resolve(),
        )

    @classmethod
    def _parse_user_selection(
        cls, user_selected_modules: list[str], organization_dir: Path
    ) -> tuple[set[RelativeDirPath | str], list[str]]:
        selected: set[RelativeDirPath | str] = set()
        errors: list[str] = []
        for item in user_selected_modules:
            if "/" not in item:
                # Module name provided
                selected.add(item)
                continue

            item_path = Path(item)
            if item_path.is_absolute():
                errors.append(
                    f"Selected module path {item_path.as_posix()!r} should be relative to the organization directory"
                )
                continue
            absolute_path = organization_dir / item_path
            if not absolute_path.exists():
                errors.append(
                    f"Selected module path {item_path.as_posix()!r} does not exist under the organization directory"
                )
                continue
            if not absolute_path.is_dir():
                errors.append(f"Selected module path {item_path.as_posix()!r} is not a directory")
                continue
            selected.add(item_path)
        return selected, errors

    def _prepare_build_directory(self, build_dir: Path) -> None:
        """Ensures the build directory is clean before a build."""
        if build_dir.exists():
            safe_rmtree(build_dir)
        build_dir.mkdir(parents=True)
        return None

    def _build_modules(
        self, module_sources: Sequence[ModuleSource], build_dir: Path, console: Console
    ) -> list[BuiltModule]:
        built_modules: list[BuiltModule] = []
        # If parallelizing the build, this should be a multiprocessing.Manager().Counter() or similar.
        resource_counter: Counter = Counter()
        # and use one orchestrator per process
        validator = LocalRulesOrchestrator(exclude_rule_codes=None, enable_alpha_validators=False)

        with Progress(console=console) as progress:
            total_files = sum(source.total_files for source in module_sources)
            build_task = progress.add_task("Building modules", total=total_files)
            for source in module_sources:
                module_name = source.name
                progress.update(build_task, description=f"Building {module_name}")

                # Inside this loop, do not raise exceptions.
                module = self._import_module(source)

                # Local validation of module
                insights = validator.run(module)
                built_resources = self._export_resources(module.files, resource_counter, build_dir)

                built_modules.append(
                    BuiltModule(
                        module_id=module.id,
                        resources=built_resources,
                        insights=insights,
                        syntax_warnings_by_source={
                            file.source_path: file.syntax_warning
                            for file in module.files
                            if isinstance(file, SuccessfulReadYAMLFile) and file.syntax_warning is not None
                        },
                        failed_files=[file for file in module.files if isinstance(file, FailedReadYAMLFile)],
                        ignored_files=module.ignored_files,
                        unresolved_variables_by_source={
                            file.source_path: file.unresolved_variables
                            for file in module.files
                            if file.unresolved_variables
                        },
                        yaml_line_count=sum(
                            file.line_count for file in module.files if isinstance(file, SuccessfulReadYAMLFile)
                        ),
                    )
                )
                progress.update(build_task, description=f"Built {module_name}", advance=source.total_files)
            progress.update(build_task, description=f"Finished building. Built {len(built_modules)} modules")
        return built_modules

    def _import_module(self, source: ModuleSource) -> Module:
        resources: list[ReadYAMLFile] = []
        ignored_files: list[IgnoredFile] = []
        for resource_folder, resource_files in source.resource_files_by_folder.items():
            crud_classes = RESOURCE_CRUD_BY_FOLDER_NAME.get(resource_folder)
            if not crud_classes:
                # This is handled in the module parsing phase.
                continue
            class_by_kind = {crud_class.kind.lower(): crud_class for crud_class in crud_classes}
            for resource_file in resource_files:
                if "." not in resource_file.stem:
                    ignored_files.append(
                        IgnoredFile(
                            filepath=resource_file,
                            code="MISSING-SUFFIX",
                            reason=f"Resource file '{resource_file.stem!r}' does not have a suffix to determine the resource kind. It is ignored.",
                        )
                    )
                    continue
                kind = resource_file.stem.rsplit(".", maxsplit=1)[-1]
                kind_key = kind.lower()
                if kind_key not in class_by_kind:
                    resources.append(
                        FailedReadYAMLFile(
                            source_path=resource_file,
                            code="INVALID-KIND",
                            error=f"Resource file '{resource_file.name!r}' has unknown resource kind '{kind}' for folder '{resource_folder}'",
                        )
                    )
                    continue
                resources.append(self._read_resource_file(resource_file, class_by_kind[kind_key], source.variables))
        return Module(id=source.as_id(), files=resources, ignored_files=ignored_files)

    def _read_resource_file(
        self,
        resource_file: AbsoluteFilePath,
        crud_class: type[ResourceIO],
        variables: list[BuildVariable],
    ) -> ReadYAMLFile:
        try:
            # The file hash has to be calculated here as the .safe_read
            # modifies the content for certain kinds of resources such at for example data modeling resources that have
            # version.
            file_hash = calculate_hash(resource_file, shorten=True)
            content = crud_class.safe_read(resource_file)
        except Exception as read_error:
            return FailedReadYAMLFile(
                source_path=resource_file, error=f"Failed to read resource file: {read_error!s}", code="READ-ERROR"
            )

        # Content read successfully.
        substituted_content = content
        line_count = content.count("\n") + (1 if content and not content.endswith("\n") else 0)
        if variables:
            substituted_content = crud_class.substitute_variables_content(content, variables)

        unresolved_variables = self._find_unresolved_variables(substituted_content)
        try:
            parsed_yaml = read_yaml_content(substituted_content)
        except yaml.YAMLError as yaml_error:
            if unresolved_variables:
                error = (
                    f"Failed to parse YAML content. "
                    f"This is likely due to unresolved variables: {humanize_collection(unresolved_variables)!s}.\n"
                    f"Error: {yaml_error!s}"
                )
            else:
                error = f"Failed to parse YAML content.\n{yaml_error!s}"
            return FailedReadYAMLFile(
                source_path=resource_file,
                code="YAML-PARSE-ERROR",
                error=error,
                unresolved_variables=unresolved_variables,
            )

        if parsed_yaml is None:
            return FailedReadYAMLFile(
                source_path=resource_file,
                code="EMPTY-YAML",
                error="The YAML file is empty. Please add content to the file or remove it if it is not needed.",
                unresolved_variables=unresolved_variables,
            )

        resource_type = ResourceType(resource_folder=crud_class.folder_name, kind=crud_class.kind)
        args: dict[str, Any] = dict(
            source_path=resource_file,
            source_hash=file_hash,
            resource_type=resource_type,
            line_count=line_count,
        )

        if isinstance(parsed_yaml, dict):
            toolkit_resource: ToolkitResource | None = None
            syntax_warning: ModelSyntaxWarning | None = None
            try:
                toolkit_resource = crud_class.yaml_cls.model_validate(parsed_yaml, extra="forbid")
                identifier = toolkit_resource.as_id()
            except ValidationError as errors:
                syntax_warning = self._create_syntax_warning(errors)
                identifier = crud_class.get_id(parsed_yaml)

            extra_files = self._substitute_variables_extra_content(
                crud_class.get_extra_files(resource_file, identifier, parsed_yaml), variables
            )

            return SuccessfulReadYAMLFile(
                syntax_warning=syntax_warning,
                resources=[
                    ReadResource(
                        raw=parsed_yaml, identifier=identifier, validated=toolkit_resource, extra_files=extra_files
                    )
                ],
                unresolved_variables=unresolved_variables,
                **args,
            )
        # Is instance list
        # MyPy complains as the yaml_cls type is determined at runtime,
        # and thus not available to te static type checker.
        adapter = TypeAdapter[list[crud_class.yaml_cls]](list[crud_class.yaml_cls])  # type: ignore[name-defined]
        toolkit_resources: list[ToolkitResource] = []
        syntax_warning = None
        try:
            toolkit_resources = adapter.validate_python(parsed_yaml)
        except ValidationError as errors:
            syntax_warning = self._create_syntax_warning(errors)
        read_resources: list[ReadResource[ToolkitResource]] = []
        for tk_resource, raw in zip_longest(toolkit_resources, parsed_yaml, fillvalue=None):
            if tk_resource is None:
                identifier = crud_class.get_id(raw)
            else:
                identifier = tk_resource.as_id()
            # We know that the parse_yaml list will always be longer than tk_resource
            # thus raw will never be None.
            raw_dict = cast(dict[str, Any], raw)
            extra_files = self._substitute_variables_extra_content(
                crud_class.get_extra_files(resource_file, identifier, raw_dict), variables
            )
            read_resources.append(
                ReadResource(
                    raw=raw_dict,
                    identifier=identifier,
                    validated=tk_resource,
                    extra_files=extra_files,
                )
            )
        return SuccessfulReadYAMLFile(
            syntax_warning=syntax_warning, resources=read_resources, **args, unresolved_variables=unresolved_variables
        )

    @classmethod
    def _find_unresolved_variables(cls, content: str) -> list[str]:
        return [
            # Removing the '{{' and '}}'
            variable[2:-2].strip()
            for variable in re.findall(pattern=r"\{\{.*?\}\}", string=content)
        ]

    @classmethod
    def _substitute_variables_extra_content(
        cls, extra_files: Iterable[ReadExtra], variables: list[BuildVariable]
    ) -> list[ReadExtra]:
        output: list[ReadExtra] = []
        for extra_file in extra_files:
            if (
                isinstance(extra_file, SuccessExtra)
                and extra_file.content
                and extra_file.suffix in SUPPORTS_VARIABLE_REPLACEMENT
            ):
                # We check that it is a valid suffix above.
                extra_file.content = BuildVariable.substitute(extra_file.content, variables, extra_file.suffix)  # type: ignore[arg-type]
            output.append(extra_file)
        return output

    def _create_syntax_warning(self, error: ValidationError) -> ModelSyntaxWarning:
        errors = humanize_validation_error(error)
        error_str = " - ".join(errors)
        return ModelSyntaxWarning(
            code="MODEL-SYNTAX-WARNING",
            message=f"The resource definition has syntax errors:\n{error_str}",
            fix="Make sure the resource YAML content is valid and follows the expected structure.",
        )

    def _export_resources(
        self, files: Sequence[ReadYAMLFile], resource_counter: Counter, build_dir: Path
    ) -> list[BuiltResource]:
        built_resources: list[BuiltResource] = []
        for file in files:
            if not isinstance(file, SuccessfulReadYAMLFile):
                continue
            folder = build_dir / file.resource_type.resource_folder
            folder.mkdir(parents=True, exist_ok=True)
            for resource in file.resources:
                resource_counter.update([file.resource_type])
                index = resource_counter[file.resource_type]
                source_stem = file.source_path.stem.rsplit(".", maxsplit=1)[0]
                identifier_filename = resource.identifier.as_filename(include_type=False)
                filestem = f"{index}-{source_stem}-{identifier_filename}"
                filename = f"{filestem}.{file.resource_type.kind}.yaml"
                destination_path = folder / filename
                safe_write(destination_path, yaml_safe_dump(resource.raw), encoding=BUILD_FOLDER_ENCODING)
                for extra_file in resource.extra_files:
                    if not isinstance(extra_file, SuccessExtra):
                        continue
                    extra_path = folder / f"{filestem}{extra_file.suffix}"
                    if extra_file.content:
                        safe_write(extra_path, extra_file.content, encoding=BUILD_FOLDER_ENCODING)
                    elif extra_file.byte_content:
                        extra_path.write_bytes(extra_file.byte_content)
                    else:
                        shutil.copy2(extra_file.source_path, extra_path)

                crud_cls = file.resource_type.crud_cls
                if resource.validated:
                    dependencies = set(crud_cls.get_dependencies(resource.validated))
                else:
                    dependencies = set()

                built_resources.append(
                    BuiltResource(
                        identifier=resource.identifier,
                        type=file.resource_type,
                        source_hash=file.source_hash,
                        source_path=file.source_path,
                        build_path=destination_path,
                        crud_cls=file.resource_type.crud_cls,
                        dependencies=dependencies,
                        failed_extra=[extra for extra in resource.extra_files if isinstance(extra, FailedReadExtra)],
                        has_syntax_error=resource.validated is None,
                    )
                )
        return built_resources

    def _create_validation_plan(
        self, built_modules: list[BuiltModule], client: ToolkitClient | None
    ) -> list[ValidationStep]:
        all_rules = get_global_rules_registry()
        plan: list[ValidationStep] = []
        for rule_cls in all_rules:
            rule = rule_cls(built_modules, client=client)

            status = rule.get_status()
            plan.append(ValidationStep(status=status, rule=rule))
        return plan

    def _display_validation_plan(self, plan: list[ValidationStep], console: Console) -> None:
        ready_count = sum(1 for step in plan if step.status.code == "ready")
        skip_count = sum(1 for step in plan if step.status.code == "skip")
        unavailable_count = sum(1 for step in plan if step.status.code == "unavailable")

        summary_lines = [f"[green]✓[/] [bold]{ready_count}[/] validations ready to run"]
        border_color = 0
        if skip_count:
            summary_lines.append(f"[yellow]![/] [bold]{skip_count}[/] validations skipped")
            border_color = max(border_color, 1)
        if unavailable_count:
            summary_lines.append(f"[yellow]![/] [bold]{unavailable_count}[/] validations unavailable")
            border_color = max(border_color, 1)

        border_style = {0: "green", 1: "yellow", 2: "red"}[border_color]
        console.print(
            ToolkitPanel(
                "\n".join(summary_lines),
                title="[bold]Validation Plan[/]",
                border_style=border_style,
                expand=False,
            )
        )

        table = Table(title="Validation Steps", expand=False, show_edge=False)
        table.add_column("Validation", style="bold")
        table.add_column("Status", style="dim")
        table.add_column("Message", style="dim")
        for step in plan:
            status_style = {"ready": "green", "reduced": "yellow", "skip": "yellow", "unavailable": "red"}[
                step.status.code
            ]
            status_display = f"[{status_style}]{step.status.code}[/]"
            message = step.status.message or "-"
            table.add_row(step.rule.DISPLAY_NAME, status_display, message)
        console.print(table)
        return None

    def _run_validation(self, plan: list[ValidationStep], console: Console) -> list[ValidationResult]:
        with Progress(console=console) as progress:
            ready_step_count = sum(1 for step in plan if step.status.code == "ready")
            validating_task = progress.add_task("Checking modules", total=ready_step_count)
            validation_results: list[ValidationResult] = []
            for step in plan:
                if step.status.code != "ready":
                    continue
                display_name = step.rule.DISPLAY_NAME
                progress.update(validating_task, description=f"Checking {display_name}...")

                insights: list[Insight] = []
                failures: list[FailedValidation] = []
                for result in step.rule.validate():
                    if isinstance(result, FailedValidation):
                        failures.append(result)
                    else:
                        insights.append(result)

                validation_results.append(ValidationResult(name=display_name, insights=insights, failed=failures))
                progress.update(validating_task, advance=1, description=f"Finished checking {display_name}.")
            progress.update(validating_task, description=f"Finished checking. Ran {ready_step_count} validations.")
        return validation_results

    def _display_insights(self, insights: InsightList, insight_path: Path, console: Console, verbose: bool) -> None:
        if not insights:
            return

        console.print("\n[bold]Build Insights[/bold]")

        display_insights = self._select_display_insights(insights, max_display_count=30 if verbose else 5)
        remaining_count = len(insights) - len(display_insights)

        # Map severity to style information
        severity_style = {
            "FileReadError": ("red", "✗"),
            "ConsistencyError": ("red", "✗"),
            "ModelSyntaxWarning": ("yellow", "!"),
            "Recommendation": ("blue", "🛈"),
            "IgnoredFileWarning": ("dim", "○"),
        }

        for insight in reversed(display_insights):
            insight_type_name = type(insight).__name__
            style, icon = severity_style.get(insight_type_name, ("white", "•"))

            # Build the content for this insight
            content_lines = [f"[bold]{insight.message}[/bold]"]
            if insight.fix:
                content_lines.append(f"\n[dim]Fix:[/dim] {insight.fix}")

            panel_title = f"[{style}]{icon}[/] [{style}]{insight_type_name}[/]"
            if insight.code:
                panel_title += f" [dim]({insight.code})[/dim]"

            console.print(
                ToolkitPanel(
                    "\n".join(content_lines),
                    title=panel_title,
                    border_style=style,
                    expand=False,
                )
            )

        insight_destination = relative_to_if_possible(insight_path)
        footer = f"All insights are written to {insight_destination.as_posix()}"
        suffix = ""
        if not verbose:
            suffix = " Add --verbose to show more."
        if remaining_count > 0:
            footer = f"... and {remaining_count} more insights not shown.{suffix} {footer}"
        console.print(f"[dim]{footer}[/dim]")

    def _select_display_insights(self, insights: InsightList, max_display_count: int) -> list[Insight]:
        """Prioritize one insight per code, then by severity"""
        insights_by_code: dict[str, Insight] = {}
        remaining_insights: list[Insight] = []

        for insight in insights:
            code = insight.code or "UNDEFINED"
            if code not in insights_by_code:
                insights_by_code[code] = insight
            else:
                remaining_insights.append(insight)

        # Sort the unique codes by severity
        sorted_unique_insights = sorted(insights_by_code.values(), key=lambda i: type(i).severity, reverse=True)
        # Sort remaining by severity
        sorted_remaining = sorted(remaining_insights, key=lambda i: type(i).severity, reverse=True)
        # Combine them
        prioritized_insights = sorted_unique_insights + sorted_remaining
        return sorted(
            prioritized_insights[:max_display_count], key=lambda i: (type(i).severity, i.code or ""), reverse=True
        )

    def _display_build_summary(
        self, build_folder: BuildFolder, insights: InsightList, console: Console, verbose: bool
    ) -> None:
        module_count = len(build_folder.built_modules)
        resource_count = sum(len(module.resources) for module in build_folder.built_modules)
        resource_type_count = len(
            {resource.type for module in build_folder.built_modules for resource in module.resources}
        )
        summary_lines = [
            f"[green]✓[/] [bold]{module_count}[/] modules",
            f"[green]✓[/] [bold]{resource_count}[/] resources of {resource_type_count} different types.",
        ]
        aggregates = Counter((insight.insight_type(), type(insight).severity) for insight in insights)
        max_severity = 0
        for (insight_type, severity), count in sorted(aggregates.items(), key=lambda i: i[1], reverse=True):
            max_severity = max(max_severity, severity)
            match severity:
                case severity if severity <= 15:
                    insight_style = "[green]✓[/]"
                case severity if 15 < severity <= 35:
                    insight_style = "[yellow]![/]"
                case _:
                    insight_style = "[red]✗[/]"

            summary_lines.append(f"{insight_style} [bold]{count}[/] {insight_type}")

        build_dir_display = relative_to_if_possible(build_folder.build_dir).as_posix()
        if not build_dir_display.endswith("/"):
            build_dir_display = f"{build_dir_display}/"

        match max_severity:
            case severity if severity <= 15:
                border_color = "green"
                recommendation = "[green]✓[/] [bold]Ready to deploy.[/bold]\nNo critical errors found. You can proceed with deployment."
            case severity if 15 < severity <= 35:
                recommendation = (
                    "[yellow]![/] [bold]Proceed with caution.[/bold]\n"
                    "There are model syntax warnings. Deployment may fail for some resources."
                )
                border_color = "orange1"
            case _:
                recommendation = (
                    "[red]✗[/] [bold]Do not proceed to deploy.[/bold]\n"
                    "There are YAML parsing errors that must be fixed before deployment."
                )
                border_color = "red"
        summary_lines.append("")
        summary_lines.append(recommendation)
        console.print(
            ToolkitPanel(
                "\n".join(summary_lines),
                title=f"[bold]Built to directory {build_dir_display}[/]",
                border_style=border_color,
                expand=False,
            )
        )

        return None

    def _track_build_results(
        self, build_folder: BuildFolder, insights: InsightList, client: ToolkitClient | None = None
    ) -> None:
        built_resources = [resource for module in build_folder.built_modules for resource in module.resources]
        duration_ms = int((build_folder.finished_at - build_folder.started_at).total_seconds() * 1000)

        resource_counts: Counter[str] = Counter(
            f"{to_tracking_key(f'{resource.type.resource_folder} {resource.type.kind}')}Built"
            for resource in built_resources
        )
        dependency_total = sum(len(resource.dependencies) for resource in built_resources)
        built_count = len(built_resources)
        dependency_average = round((dependency_total / built_count), 6) if built_count else 0.0

        insight_codes_set = {ins.code if ins.code is not None else "UNDEFINED" for ins in insights}
        yaml_line_count = sum(module.yaml_line_count for module in build_folder.built_modules)

        payload: dict[str, Any] = {
            "build_duration_ms": duration_ms,
            "resource_types": sorted(resource_counts.keys()),
            "insight_codes": sorted(insight_codes_set),
            "dependency_total": dependency_total,
            "dependency_average": dependency_average,
            "built_resource_total": built_count,
            "module_count": len(build_folder.built_modules),
            "insight_total_count": len(insights),
            "yaml_line_count": yaml_line_count,
        }
        payload.update(resource_counts)
        event = BuildTracking.model_validate(payload)
        self.tracker.track(event, client)

    def _write_results(
        self, insights: InsightList, build: BuildFolder, parameters: BuildParameters, cdf_project: str | None = None
    ) -> None:
        """Write build results including lineage information and insights to the build folder."""

        insight_file = parameters.insight_path
        if parameters.insight_format == "csv":
            insight_file_content = insights.to_csv()
        else:
            insight_file_content = insights.to_json()
        if insight_file_content.strip():
            safe_write(insight_file, insight_file_content)

        lineage_file = build.build_dir / BuildLineage.filename
        lineage = BuildLineage.from_build(build, cdf_project).to_yaml()
        safe_write(lineage_file, lineage)
