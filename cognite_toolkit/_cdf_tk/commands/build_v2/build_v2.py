import os
import re
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import zip_longest
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import JsonValue, TypeAdapter, ValidationError
from rich.console import Console
from rich.panel import Panel
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
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ReadExtra, SuccessExtra
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
    ToolkitFileNotFoundError,
    ToolkitNotADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.rules import LocalRulesOrchestrator, ToolkitGlobalRulSet, get_global_rules_registry
from cognite_toolkit._cdf_tk.rules._base import FailedValidation, RuleSetStatus
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
    rule: ToolkitGlobalRulSet


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuildFolder:
        console = client.console if client else Console(markup=True)

        # Track build duration
        build_start_time = datetime.now(timezone.utc)

        self._validate_build_parameters(parameters, console, sys.argv)
        build_files = self._read_file_system(parameters)

        build_source = self._find_modules(build_files)
        self._display_module_sources(build_source, console, parameters.verbose)

        self._prepare_build_directory(parameters.build_dir)
        built_modules = self._build_modules(build_source.modules, parameters.build_dir, console)

        plan = self._create_validation_plan(built_modules, client)
        if parameters.verbose:
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

        self._display_build_folder(build_folder, parameters.config_yaml_name or "", console, parameters.verbose)

        self._write_results(build_folder)

        # Todo: Some mixpanel tracking.
        return build_folder

    @classmethod
    def _validate_build_parameters(cls, parameters: BuildParameters, console: Console, user_args: list[str]) -> None:
        """Checks that the user has the correct folders set up and that the config file (if provided) exists."""

        # Set up the variables
        organization_dir = parameters.organization_dir
        config_yaml_path: Path | None = None
        if parameters.config_yaml_name:
            config_yaml_path = organization_dir / ConfigYAML.get_filename(parameters.config_yaml_name)
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
        expected_panel = Panel(
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
        return ModuleParser.parse(build)

    def _display_module_sources(self, build_source: BuildSource, console: Console, verbose: bool) -> None:
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

        summary_lines = [
            f"[green]✓[/] [bold]{module_count}[/] modules",
            f"[green]✓[/] [bold]{total_files}[/] total resource files",
            f"[green]✓[/] [bold]{resource_type_count}[/] resource types",
        ]
        border_color = 0
        errors: list[str] = []
        if read_variables:
            summary_lines.append(f"[green]✓[/] [bold]{read_variables}[/] variables used in the build")
        if ambiguous_selected_count:
            summary_lines.append(
                f"[red]✗[/] [bold]{ambiguous_selected_count}[/] user-selected modules had an ambiguous match with multiple module directories."
            )
            border_color = 2
            errors.append("ambiguous selected")
        if misplaced_modules_count:
            summary_lines.append(
                f"[red]✗[/] [bold]{misplaced_modules_count}[/] modules are located directly under the another module."
            )
            border_color = 2
            errors.append("misplaced modules")
        if non_existing_module_count:
            summary_lines.append(
                f"[red]✗[/] [bold]{non_existing_module_count}[/] user-selected module names did not match any module directory."
            )
            border_color = 2
            errors.append("non existing modules")
        if invalid_variable_count:
            summary_lines.append(
                f"[red]✗[/] [bold]{invalid_variable_count}[/] invalid variables found across modules and config YAML."
            )
            border_color = 2
            errors.append("invalid variables")
        if orphan_yaml_count:
            summary_lines.append(
                f"[yellow]![/] [bold]{orphan_yaml_count}[/] YAML files found directly under the modules directory that are not part of any module."
            )
            border_color = max(border_color, 1)
        border_style = {0: "green", 1: "yellow", 2: "red"}[border_color]
        module_dir_display = relative_to_if_possible(build_source.module_dir)
        console.print(
            Panel(
                "\n".join(summary_lines),
                title=f"[bold]Read module dir ({module_dir_display.as_posix()})[/]",
                border_style=border_style,
                expand=False,
            )
        )

        # Print detailed issue information
        if ambiguous_selected_count:
            table = Table(title="Ambiguous Module Selections", expand=False, show_edge=False)
            table.add_column("Module Name", style="red")
            table.add_column("Matching Paths", style="dim")
            for selection in build_source.ambiguous_selection:
                if selection.is_selected:
                    paths_str = ", ".join(p.as_posix() for p in selection.module_paths)
                    table.add_row(selection.name, paths_str)
            console.print(table)

        if misplaced_modules_count:
            table = Table(title="Misplaced Modules", expand=False, show_edge=False)
            table.add_column("Module Path", style="red")
            table.add_column("Parent Modules", style="dim")
            for misplaced in build_source.misplaced_modules:
                parents_str = ", ".join(p.as_posix() for p in misplaced.parent_modules)
                table.add_row(misplaced.id.as_posix(), parents_str)
            console.print(table)

        if non_existing_module_count:
            table = Table(title="Non-Existing Module Names", expand=False, show_edge=False)
            table.add_column("Module Name", style="red")
            table.add_column("Closest Matches", style="dim")
            for non_existing in build_source.non_existing_module_names:
                matches_str = ", ".join(non_existing.closest_matches) if non_existing.closest_matches else "-"
                table.add_row(non_existing.name, matches_str)
            console.print(table)

        if invalid_variable_count:
            table = Table(title="Invalid Variables", expand=False, show_edge=False)
            table.add_column("Variable Path", style="red")
            table.add_column("Error", style="dim")
            for invalid_var in build_source.invalid_variables:
                table.add_row(invalid_var.id.as_posix(), invalid_var.error.message)
            console.print(table)

        if verbose and orphan_yaml_count:
            table = Table(title="Orphan YAML Files", expand=False, show_edge=False)
            table.add_column("File Path", style="yellow")
            for orphan_file in build_source.orphan_yaml_files:
                display_path = relative_to_if_possible(orphan_file)
                table.add_row(display_path.as_posix())
            console.print(table)

        if errors:
            console.print("\n")
            raise ToolkitValueError(
                f"Cannot build {module_dir_display.as_posix()!r}. You are not allowed to have"
                f" {humanize_collection(errors)}."
            )
        return None

    @classmethod
    def _read_file_system(cls, parameters: BuildParameters) -> BuildSourceFiles:
        """Reads the file system to find the YAML files to build along with config.<name>.yaml if it exists."""
        selected: set[RelativeDirPath | str] = {
            parameters.modules_directory.relative_to(parameters.organization_dir)
        }  # Default to everything under modules.
        variables: dict[str, JsonValue] = {}
        cdf_project: str = os.environ.get("CDF_PROJECT", "UNKNOWN")
        validation_type: ValidationType = "prod"
        if parameters.user_selected_modules:
            selected, errors = cls._parse_user_selection(parameters.user_selected_modules, parameters.organization_dir)
            if errors:
                raise ToolkitValueError("Invalid module selection:\n" + "\n".join(f"- {error}" for error in errors))

        if parameters.config_yaml_name:
            try:
                config = ConfigYAML.from_yaml_file(
                    ConfigYAML.get_filepath(parameters.organization_dir, parameters.config_yaml_name)
                )
            except ValidationError as e:
                errors = humanize_validation_error(e)
                raise ToolkitValueError(
                    f"Config YAML file '{parameters.config_yaml_name}' is invalid:\n{'- '.join(errors)}"
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
        validator = LocalRulesOrchestrator(
            exclude_rule_codes=None,
            enable_alpha_validators=False,
        )

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
        crud_class: type[ResourceCRUD],
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
            Panel(
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
            status_style = {"ready": "green", "skip": "yellow", "unavailable": "yellow"}[step.status.code]
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

    def _display_build_folder(
        self, build_folder: BuildFolder, config_yaml_name: str, console: Console, verbose: bool
    ) -> None:
        module_count = len(build_folder.built_modules)
        resource_count = sum(len(module.resources) for module in build_folder.built_modules)
        resource_type_count = len(
            {resource.type for module in build_folder.built_modules for resource in module.resources}
        )
        syntax_warning_count = sum(len(module.syntax_warnings_by_source) for module in build_folder.built_modules)
        failed_read_files = [file for module in build_folder.built_modules for file in module.failed_files]
        failed_read_file_count = len(failed_read_files)
        ignored_files = [file for module in build_folder.built_modules for file in module.ignored_files]
        ignore_file_count = len(ignored_files)

        summary_lines = [
            f"[green]✓[/] [bold]{module_count}[/] modules",
            f"[green]✓[/] [bold]{resource_count}[/] resources of {resource_type_count} different types.",
        ]
        border_color = 0
        if failed_read_file_count:
            summary_lines.append(
                f"[red]✗[/] [bold]{failed_read_file_count}[/] resource files failed to be read.\n    These files are ignored in the build, but you should fix the issues to\n"
                f"    ensure all your resources are included in the build."
            )
            border_color = max(border_color, 2)
        if ignored_files:
            most_common = Counter([file.code for file in ignored_files]).most_common(3)
            most_common_str = humanize_collection([f"{code} ({count} files)" for code, count in most_common])
            summary_lines.append(
                f"[yellow]![/] [bold]{ignore_file_count}[/] resource files were ignored. The most common reasons {most_common_str}."
            )
            border_color = max(border_color, 1)

        if syntax_warning_count:
            summary_lines.append(
                f"[red]✗[/] [bold]{syntax_warning_count}[/] syntax warnings found in resource files. The resources have still been built, but you should fix the syntax issues to avoid potential problems when deploying the resources."
            )
            border_color = max(border_color, 1)

        for validation in build_folder.validation_results:
            if validation.failed:
                prefix = "[red]✗[/]"
            elif validation.insights:
                prefix = "[yellow]![/]"
                border_color = max(border_color, 1)
            else:
                prefix = "[green]✓[/]"
            suffix: list[str] = []
            if validation.failed:
                suffix.append(f"failed {len(validation.failed)} checks")
            if validation.insights:
                suffix.append(f"found {len(validation.insights)} insights")
            if not validation.failed and not validation.insights:
                suffix.append("all checks passed")
            summary_lines.append(f"{prefix} {validation.name} {humanize_collection(suffix, sort=False)}.")

        build_dir_display = relative_to_if_possible(build_folder.build_dir).as_posix()
        if not build_dir_display.endswith("/"):
            build_dir_display = f"{build_dir_display}/"
        console.print(
            Panel(
                "\n".join(summary_lines),
                title=f"[bold]Built to directory {build_dir_display}[/]",
                border_style={0: "green", 1: "yellow", 2: "red"}[border_color],
                expand=False,
            )
        )

        all_insights = build_folder.all_insights
        if all_insights:
            table = Table(title="Insights", expand=False, show_edge=False)
            table.add_column("Type", style="dim")
            table.add_column("Code", style="dim")
            table.add_column("Description", style="dim")
            table.add_column("Fix", style="dim")
            max_reached = False
            for no, issue in enumerate(all_insights):
                table.add_row(type(issue).__name__, issue.code or "", issue.message, issue.fix or "-")
                if no > 10:
                    max_reached = True
                    break
            console.print(table)
            if max_reached:
                console.print(
                    f"[dim]... and {len(all_insights) - 10} more insights not shown[/]",
                    style="dim",
                )

        if verbose and ignored_files:
            table = Table(title="Ignored Files", expand=False, show_edge=False)
            table.add_column("Code", style="bold")
            table.add_column("Path", style="bold")
            table.add_column("Reason", style="bold")
            for file in ignored_files:
                table.add_row(file.code, relative_to_if_possible(file.filepath).as_posix(), file.reason)
            console.print(table)

        if failed_read_file_count:
            available_variable_names = {variable.name for variable in build_folder.all_variables}
            table = Table(title="Failed Read Files", expand=False, show_edge=True, border_style="red", show_lines=True)
            table.add_column(
                "Code",
            )
            table.add_column("Description")
            table.add_column("File Path")
            table.add_column("Fix")
            for failed_file in failed_read_files:
                display_path = relative_to_if_possible(failed_file.source_path).as_posix()
                fix = ""
                if misplaced := (set(failed_file.unresolved_variables) & available_variable_names):
                    fix = (
                        f"Unresolved variable(s) {humanize_collection(misplaced)} are likely misplaced in the config.{config_yaml_name}.yaml.\n"
                        "Make sure they are placed correctly in the variables section matching the file path."
                    )

                table.add_row(failed_file.code, failed_file.error, display_path, fix)
            console.print(table)
            raise ToolkitError(f"Cannot continue with {failed_read_file_count} failed read YAML files.")

        return None

    def _write_results(self, build: BuildFolder) -> None:
        """Write build results including lineage information and insights to the build folder."""

        insight_file = build.build_dir / "insights.csv"

        insight_file_content = build.all_insights.to_csv()
        if insight_file_content.strip():
            safe_write(insight_file, insight_file_content)

        lineage_file = build.build_dir / BuildLineage.filename
        lineage = BuildLineage.from_build(build).to_yaml()
        safe_write(lineage_file, lineage)
