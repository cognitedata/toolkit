import os
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
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
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ViewNoVersionId
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2._module_source_parser import ModuleSourceParser
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
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    InsightList,
    ModelSyntaxWarning,
)
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
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._plugins import NeatPlugin
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING, HINT_LEAD_TEXT, MODULES
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ReadExtra, SuccessExtra
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.rules import RulesOrchestrator
from cognite_toolkit._cdf_tk.utils import calculate_hash, safe_write
from cognite_toolkit._cdf_tk.utils.file import (
    read_yaml_content,
    relative_to_if_possible,
    safe_rmtree,
    yaml_safe_dump,
)
from cognite_toolkit._cdf_tk.validation import humanize_validation_error
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuildFolder:
        console = client.console if client else Console(markup=True)

        # Track build duration
        build_start_time = datetime.now(timezone.utc)

        self._validate_build_parameters(parameters, console, sys.argv)
        build_files = self._read_file_system(parameters)

        build_source = self._find_modules(build_files)
        self._display_module_sources(build_source, console)

        self._prepare_build_directory(parameters.build_dir)
        built_modules = self._build_modules(build_source.modules, parameters.build_dir, console)

        dependency_insights = self._dependency_validation(built_modules, client)

        global_insights = self._global_validation(built_modules, client)

        # Calculate build duration
        build_duration_seconds = round((datetime.now(timezone.utc) - build_start_time).total_seconds(), 2)

        build_folder = BuildFolder(
            path=parameters.build_dir,
            built_modules=built_modules,
            dependency_insights=dependency_insights,
            global_insights=global_insights,
        )

        self._display_build_folder(build_folder, console)

        self._write_results(parameters, build_folder, build_start_time, build_duration_seconds)

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
        parser = ModuleSourceParser(build.selected_modules, build.organization_dir)
        module_sources = parser.parse(build.yaml_files, build.variables)
        return BuildSource(
            module_dir=build.module_dir,
            modules=module_sources,
            insights=parser.errors,
        )

    def _display_module_sources(self, build_source: BuildSource, console: Console) -> None:
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

        summary_lines = [
            f"[green]✓[/] [bold]{module_count}[/] modules",
            f"[green]✓[/] [bold]{total_files}[/] total resource files",
            f"[green]✓[/] [bold]{resource_type_count}[/] resource types",
        ]
        if read_variables:
            summary_lines.append(f"[green]✓[/] [bold]{read_variables}[/] variables used in the build")

        has_issues = False
        if build_source.insights:
            summary_lines.append(f"[red]✗[/] [bold]{len(build_source.insights)}[/] issues found while reading modules")
            has_issues = True
        module_dir_display = relative_to_if_possible(build_source.module_dir)
        console.print(
            Panel(
                "\n".join(summary_lines),
                title=f"[bold]Read module dir ({module_dir_display.as_posix()})[/]",
                border_style="yellow" if has_issues else "green",
                expand=False,
            )
        )
        if build_source.insights:
            table = Table(title="Reading module issues", expand=False, show_edge=False)
            table.add_column("Type", style="dim")
            table.add_column("Code", style="dim")
            table.add_column("Description", style="dim")
            table.add_column("Fix", style="dim")
            for issue in build_source.insights:
                table.add_row(type(issue).__name__, issue.code or "", issue.message, issue.fix or "-")
            console.print(table)
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

        # Todo optimize by only searching for yaml files in the selected modules paths if selection is provided.
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
        orchestrator = RulesOrchestrator()

        with Progress(console=console) as progress:
            total_files = sum(source.total_files for source in module_sources)
            build_task = progress.add_task("Building modules", total=total_files)
            for source in module_sources:
                module_name = source.name
                progress.update(build_task, description=f"Building {module_name}")

                # Inside this loop, do not raise exceptions.
                module = self._import_module(source)

                # Local validation of module
                insights = orchestrator.run(module)
                built_resources = self._export_resources(module.files, resource_counter, build_dir)

                built_modules.append(
                    BuiltModule(
                        module_id=module.id,
                        resources=built_resources,
                        insights=insights,
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

        try:
            parsed_yaml = read_yaml_content(substituted_content)
        except yaml.YAMLError as yaml_error:
            # Todo Look for variables not replaced in the content and add fix suggestion to the error.
            #  Look for variables at an adjacent level in the YAML structure to give more specific suggestions.
            #  Jira: CDF-27203
            return FailedReadYAMLFile(
                source_path=resource_file,
                code="YAML-PARSE-ERROR",
                error=f"Failed to parse YAML content: {yaml_error!s}",
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
        return SuccessfulReadYAMLFile(syntax_warning=syntax_warning, resources=read_resources, **args)

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
                        # Todo Find a better solution for syntax warnings
                        #     This solution leads to duplicates.
                        syntax_warning=file.syntax_warning,
                    )
                )
        return built_resources

    def _dependency_validation(self, built_modules: list[BuiltModule], client: ToolkitClient | None) -> InsightList:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""
        built_resource_ids: set[tuple[type[ResourceCRUD], Identifier]] = {
            (resource.crud_cls, resource.identifier) for module in built_modules for resource in module.resources
        }
        missing_locally_by_crud_cls: dict[type[ResourceCRUD], dict[Identifier, list[BuiltResource]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for module in built_modules:
            for resource in module.resources:
                for crud_cls, dependency_id in resource.dependencies:
                    if (crud_cls, dependency_id) not in built_resource_ids:
                        missing_locally_by_crud_cls[crud_cls][dependency_id].append(resource)
        insights = InsightList()
        code = "MISSING-DEPENDENCY"
        if client:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                crud = crud_cls(client, None, None)
                display_name = crud.display_name
                retrieved = crud.retrieve(list(expected_by_identifier.keys()))
                existing_in_cdf: set[Identifier] = set()
                for cdf_item in retrieved:
                    item_id = crud.get_id(cdf_item)
                    existing_in_cdf.add(item_id)
                    if crud_cls is ViewCRUD:
                        existing_in_cdf.add(ViewNoVersionId(space=item_id.space, external_id=item_id.external_id))
                if missing := set(expected_by_identifier.keys()) - existing_in_cdf:
                    for identifier in missing:
                        expected_resources = expected_by_identifier[identifier]
                        referenced_str = " - ".join(
                            f"{resource.identifier!s} in {resource.source_path.as_posix()!r}"
                            for resource in expected_resources
                        )
                        insights.append(
                            ConsistencyError(
                                code=code,
                                message=f"{identifier} {display_name} does not exist locally or in CDF. It is referenced by: \n{referenced_str}",
                                fix=f"Ensure that {display_name} exists or removed the reference to it.",
                            )
                        )

        else:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                resource_type_name = f"{crud_cls.kind.lower()} ({crud_cls.folder_name})"
                for identifier, expected_resources in expected_by_identifier.items():
                    referenced_str = " - ".join(
                        f"{resource.identifier!s} in {resource.source_path.as_posix()!r}"
                        for resource in expected_resources
                    )
                    insights.append(
                        ConsistencyError(
                            code=code,
                            message=f"{identifier} {resource_type_name} does not exist. It is referenced by: \n{referenced_str}",
                            fix=f"If the {resource_type_name} exist in CDF, provide client credentials to not get this error. "
                            f"Or ensure that {resource_type_name} exists or removed the reference to it.",
                        )
                    )

        return insights

    def _global_validation(self, built_modules: list[BuiltModule], client: ToolkitClient | None) -> InsightList:
        """This validation is performed per resource type and not per individual resource and against CDF
        for all modules. This validation will leverage external plugins such as NEAT.
        """
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        insights = InsightList()
        for built_module in built_modules:
            if not built_module.files_built:
                continue
            data_model_type = ResourceType(resource_folder=DataModelCRUD.folder_name, kind=DataModelCRUD.kind)
            if data_model_files := built_module.resource_by_type_by_kind.get(data_model_type):
                if NeatPlugin.installed() and client and data_model_files:
                    neat = NeatPlugin(client)
                    for data_model_file in data_model_files:
                        for insight in neat.validate(data_model_file.parent, data_model_file):
                            if insight not in built_module.insights:
                                insights.append(insight)
        return insights

    def _display_build_folder(self, build_folder: BuildFolder, console: Console) -> None:
        module_count = len(build_folder.built_modules)
        resource_count = sum(len(module.resources) for module in build_folder.built_modules)

        resource_insight_count = sum(
            1
            for module in build_folder.built_modules
            for resource in module.resources
            if resource.syntax_warning is not None
        )
        dependency_insight_count = len(build_folder.dependency_insights)
        global_insight_count = len(build_folder.global_insights)

        resource_type_count = len(
            {resource.type for module in build_folder.built_modules for resource in module.resources}
        )

        summary_lines = [
            f"[green]✓[/] [bold]{module_count}[/] modules",
            f"[green]✓[/] [bold]{resource_count}[/] resources",
            f"[green]✓[/] [bold]{resource_type_count}[/] resource types",
        ]
        has_issues = False
        if resource_insight_count:
            summary_lines.append(f"[yellow]![/] [bold]{resource_insight_count}[/] resource insights found.")
            has_issues = True
        if dependency_insight_count:
            summary_lines.append(f"[red]✗[/] [bold]{dependency_insight_count}[/] missing dependencies found.")
            has_issues = True
        if global_insight_count:
            summary_lines.append(f"[yellow]![/] [bold]{global_insight_count}[/] global insights found.")
            has_issues = True
        build_dir_display = relative_to_if_possible(build_folder.path)
        console.print(
            Panel(
                "\n".join(summary_lines),
                title=f"[bold]Built to ({build_dir_display.as_posix()})[/]",
                border_style="yellow" if has_issues else "green",
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
        return None

    def _write_results(
        self,
        parameters: BuildParameters,
        build_folder: BuildFolder,
        timestamp: datetime | None = None,
        duration: float | None = None,
    ) -> None:
        """Write build results including lineage information and insights to the build folder."""

        insight_file = build_folder.path / "insights.csv"
        insight_file.parent.mkdir(parents=True, exist_ok=True)

        insight_file_content = build_folder.all_insights.to_csv()
        if insight_file_content.strip():
            safe_write(insight_file, insight_file_content)

        lineage_file = build_folder.path / BuildLineage.filename
        lineage_file.parent.mkdir(parents=True, exist_ok=True)
        lineage = BuildLineage.from_build_parameters_and_results(
            parameters, build_folder, timestamp, duration
        ).to_yaml()
        safe_write(lineage_file, lineage)
