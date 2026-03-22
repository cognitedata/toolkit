import os
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import JsonValue, TypeAdapter, ValidationError
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
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
    ModelSyntaxError,
    Recommendation,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    BuildVariable,
    FailedReadResource,
    ReadResource,
    SuccessfulReadResource,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._plugins import NeatPlugin
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, MODULES
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.rules import RulesOrchestrator
from cognite_toolkit._cdf_tk.utils import calculate_hash, humanize_collection, safe_write, sanitize_filename
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, relative_to_if_possible, safe_read, yaml_safe_dump
from cognite_toolkit._cdf_tk.validation import humanize_validation_error
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuildFolder:
        console = client.console if client else Console()

        # Track build duration
        build_start_time = datetime.now(timezone.utc)

        self._validate_build_parameters(parameters, console, sys.argv)
        build_files = self._read_file_system(parameters)
        module_sources = self._parse_module_sources(build_files)

        build_folder = self._build_modules(module_sources, parameters.build_dir)

        self._cdf_dependency_validation(build_folder, client)

        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        self._global_validation(build_folder, client)

        # Calculate build duration
        build_duration_seconds = round((datetime.now(timezone.utc) - build_start_time).total_seconds(), 2)

        self._write_results(parameters, build_folder, build_start_time, build_duration_seconds)

        # Todo: Some mixpanel tracking.

        return build_folder

    @classmethod
    def _validate_build_parameters(cls, parameters: BuildParameters, console: Console, user_args: list[str]) -> None:
        """Checks that the user has the correct folders set up and that the config file (if provided) exists."""

        # Set up the variables
        organization_dir = parameters.organization_dir
        organization_dir_display = relative_to_if_possible(organization_dir)
        config_yaml_path: Path | None = None
        if parameters.config_yaml_name:
            config_yaml_path = organization_dir / ConfigYAML.get_filename(parameters.config_yaml_name)
            content = f"  ┣ {MODULES}/\n  ┗ {config_yaml_path.name}\n"
        else:
            content = f"  ┗ {MODULES}\n"
        expected_panel = Panel(
            f"Toolkit expects the following structure:\n{organization_dir_display.as_posix()!r}/\n{content}",
            expand=False,
        )
        module_directory = parameters.modules_directory

        # Execute the checks.
        if module_directory.exists() and (config_yaml_path is None or config_yaml_path.exists()):
            # All good.
            return

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

    def _parse_module_sources(self, build: BuildSourceFiles) -> list[ModuleSource]:
        parser = ModuleSourceParser(build.selected_modules, build.organization_dir)
        module_sources = parser.parse(build.yaml_files, build.variables)
        if parser.errors:
            # Todo: Nicer way of formatting errors. Jira CDF-27107
            raise ToolkitValueError(
                "Errors encountered while parsing modules:\n" + "\n".join(f"- {error!s}" for error in parser.errors)
            )
        return module_sources

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
            organization_dir=parameters.organization_dir,
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
            if not Path(item).resolve().is_relative_to(organization_dir):
                errors.append(f"Selected module path {item_path.as_posix()!r} is not under the organization directory")
                continue

            if item_path.is_absolute():
                errors.append(
                    f"Selected module path {item_path.as_posix()!r} should be relative to the organization directory"
                )
                continue
            if not (organization_dir / item_path).exists():
                errors.append(
                    f"Selected module path {item_path.as_posix()!r} does not exist under the organization directory"
                )
                continue
            if not item_path.is_dir():
                errors.append(f"Selected module path {item_path.as_posix()!r} is not a directory")
                continue
            selected.add(item_path)
        return selected, errors

    def _build_modules(self, module_sources: Sequence[ModuleSource], build_dir: Path) -> BuildFolder:
        build_dir.mkdir(parents=True, exist_ok=True)
        folder: BuildFolder = BuildFolder(path=build_dir)
        resource_counter: Counter = Counter()
        for source in module_sources:
            # Inside this loop, do not raise exceptions.
            module = self._import_module(source)  # Syntax validation
            # Todo: Avoid mutating module object.
            self._local_validation(module)
            built_module = self._export_module(module, resource_counter, build_dir)
            folder.built_modules.append(built_module)

        return folder

    def _import_module(self, source: ModuleSource) -> Module:
        resources: list[ReadResource] = []
        for resource_folder, resource_files in source.resource_files_by_folder.items():
            crud_classes = RESOURCE_CRUD_BY_FOLDER_NAME.get(resource_folder)
            if not crud_classes:
                # This is handled in the module parsing phase.
                continue
            class_by_kind = {crud_class.kind.lower(): crud_class for crud_class in crud_classes}
            for resource_file in resource_files:
                resources.extend(
                    self._import_resource_file(resource_file, class_by_kind, source.variables, resource_folder)
                )
        return Module(source=source, resources=resources)

    def _import_resource_file(
        self,
        resource_file: AbsoluteFilePath,
        class_by_kind: dict[str, type[ResourceCRUD]],
        variables: list[BuildVariable],
        folder_name: str,
    ) -> list[ReadResource]:
        if "." not in resource_file.stem:
            # Todo: Discussion error or silent ignore.
            #   Reason for error is in the case were the user do not set a kind and intends to.
            #   Reason for silently ignore is that the user for example has a YAML file as part of their
            #   function code, and it is not meant to be a resource file.
            # Todo: This will fail for kinds that just endswith the kind. This is often used for
            #   for example transformation schedules.
            return []
        kind = resource_file.stem.rsplit(".", maxsplit=1)[-1]
        crud_class = class_by_kind.get(kind.lower())
        if not crud_class:
            error = self._create_failed_read_resource_for_invalid_kind(
                resource_file, kind, folder_name, [c.kind for c in class_by_kind.values()]
            )
            return [FailedReadResource(source_path=resource_file, errors=[error])]
        error_or_string = self._read_resource_file(resource_file)
        if isinstance(error_or_string, ModelSyntaxError):
            return [FailedReadResource(source_path=resource_file, errors=[error_or_string])]
        # Content read successfully.
        if variables:
            substituted_content = self._substitute_variables_in_content(error_or_string, variables)
        else:
            substituted_content = error_or_string

        error_or_unstructured = self._parse_yaml_content(substituted_content)
        if isinstance(error_or_unstructured, ModelSyntaxError):
            return [FailedReadResource(source_path=resource_file, errors=[error_or_unstructured])]
        file_hash = calculate_hash(error_or_string, shorten=True)

        error_or_resources = self._create_resources_from_unstructured(error_or_unstructured, crud_class.yaml_cls)
        resource_type = ResourceType(resource_folder=crud_class.folder_name, kind=crud_class.kind)
        resources: list[ReadResource] = []
        for error_or_resource in error_or_resources:
            if isinstance(error_or_resource, ModelSyntaxError):
                resources.append(FailedReadResource(source_path=resource_file, errors=[error_or_resource]))
            else:
                resource, recommendations = error_or_resource
                resources.append(
                    SuccessfulReadResource(
                        source_path=resource_file,
                        resource=resource,
                        source_hash=file_hash,
                        resource_type=resource_type,
                        insights=InsightList(recommendations),
                    )
                )
        return resources

    def _create_failed_read_resource_for_invalid_kind(
        self, resource_file: Path, kind: str, resource_folder: str, available_kinds: Iterable[str]
    ) -> ModelSyntaxError:
        return ModelSyntaxError(
            code="UNKNOWN-RESOURCE-KIND",
            message=f"Resource file '{resource_file.as_posix()!r}' has unknown resource kind '{kind}' for folder '{resource_folder}'",
            fix=f"Make sure the file name ends with a known resource kind for the folder. Expected kinds for folder '{resource_folder}' are: {humanize_collection(list(available_kinds))}",
        )

    def _read_resource_file(self, resource_file: Path) -> str | ModelSyntaxError:
        try:
            return safe_read(resource_file)
        except Exception as e:
            return ModelSyntaxError(
                code="RESOURCE_FILE_READ_ERROR",
                message=f"Failed to read resource file '{resource_file.as_posix()!r}': {e!s}",
                fix="Make sure the file is a valid YAML file and is accessible.",
            )

    def _substitute_variables_in_content(self, content: str, variables: list[BuildVariable]) -> str:
        raise NotImplementedError()

    def _parse_yaml_content(self, content: str) -> dict[str, Any] | list[dict[str, Any]] | ModelSyntaxError:
        try:
            return read_yaml_content(content)
        except Exception as e:
            # Todo Look for variables not replaced in the content and add fix suggestion to the error.
            #  Look for variables at an adjacent level in the YAML structure to give more specific suggestions.
            #  Jira: CDF-27203
            return ModelSyntaxError(
                code="YAML-PARSE-ERROR",
                message=f"Failed to parse YAML content: {e!s}",
                fix="Make sure the YAML content is valid.",
            )

    def _create_resources_from_unstructured(
        self, unstructured: dict[str, Any] | list[dict[str, Any]], yaml_cls: type[ToolkitResource]
    ) -> Iterable[tuple[ToolkitResource, list[Recommendation]] | ModelSyntaxError]:
        if isinstance(unstructured, dict):
            try:
                resource = yaml_cls.model_validate(unstructured, extra="forbid")
                return [(resource, [])]  # All good.
            except ValidationError as forbid_errors:
                try:
                    # Fallback to allow extra.
                    resource = yaml_cls.model_validate(unstructured, extra="ignore")
                    return [(resource, [self._create_recommendation(forbid_errors)])]
                except ValidationError:
                    return [self._create_syntax_error(forbid_errors)]
        elif isinstance(unstructured, list):
            # MyPy complains but this work.
            adapter = TypeAdapter[list[yaml_cls]](list[yaml_cls])  # type: ignore[valid-type]
            try:
                resources = adapter.validate_python(unstructured)
                return [(resource, []) for resource in resources]  # All good.
            except ValidationError as forbid_errors:
                try:
                    # Fallback to allow extra.
                    resources = adapter.validate_python(unstructured, extra="ignore")
                    recommendation = self._create_recommendation(forbid_errors)
                    # Todo: sort recommendation by resource if possible.
                    #    Jira: CDF-27204
                    return [(resource, [recommendation]) for resource in resources]
                except ValidationError:
                    return [self._create_syntax_error(forbid_errors)]
        else:
            raise NotImplementedError(
                f"Unknown unstructured YAML content. Expected a dict or list of dicts. Got {type(unstructured)}"
            )

    def _create_recommendation(self, error: ValidationError) -> Recommendation:
        # This is a quick implementation that just creates a generic recommendation for all errors. It should be extended to create more specific recommendations based on the error details.
        errors = humanize_validation_error(error)
        return Recommendation(
            code="UNKNOWN-FIELDS",
            message=f"The resource has unknown fields: {humanize_collection(errors)}",
            fix="Remove the unknown fields from the resource definition, unless you know that they are supported by the API. "
            "In that case, you can ignore this recommendation and proceed with the deployment. If you think these fields "
            "should be supported, please reach out to the Toolkit team.",
        )

    def _create_syntax_error(self, error: ValidationError) -> ModelSyntaxError:
        errors = humanize_validation_error(error)
        return ModelSyntaxError(
            code="MODEL-SYNTAX-ERROR",
            message=f"The resource definition has syntax errors: {humanize_collection(errors)}",
            fix="Make sure the resource YAML content is valid and follows the expected structure.",
        )

    def _export_module(self, module: Module, resource_counter: Counter, build_dir: Path) -> BuiltModule:
        built_module = BuiltModule(
            source=module.source,
            module_id=module.source.as_id(),
            dependencies={},
        )
        built_resources: list[BuiltResource] = []
        for resource in module.resources:
            if not isinstance(resource, SuccessfulReadResource):
                # Todo: Allow syntax errors.
                continue
            folder = build_dir / resource.resource_type.resource_folder
            folder.mkdir(parents=True, exist_ok=True)
            resource_counter.update([resource.resource_type])
            index = resource_counter[resource.resource_type]
            source_stem = resource.source_path.stem.rsplit(".", maxsplit=1)[0]
            identifier = resource.resource.as_id()
            identifier_filename = sanitize_filename(str(identifier))
            filename = f"{index}-{source_stem}-{identifier_filename}.{resource.resource_type.kind}.yaml"
            destination_path = folder / filename
            # Todo: Store original yaml and do not require the resource. In other words allow
            #   model syntax errors.
            safe_write(
                destination_path, yaml_safe_dump(resource.resource.model_dump(by_alias=True, exclude_unset=True))
            )
            built_resources.append(
                BuiltResource(
                    identifier=identifier,
                    source_hash=resource.source_hash,
                    source_path=resource.source_path,
                    build_path=destination_path,
                    dependencies=resource.dependencies,
                    insights=resource.insights,
                )
            )
        return built_module

    @classmethod
    def _create_syntax_errors(cls, resource_type: ResourceType, error: ValidationError) -> Iterable[ModelSyntaxError]:
        # TODO: should be extended with humanizing of errors, this is a quick solution.
        for error_details in error.errors(include_input=True, include_url=False):
            message = error_details.get("msg", "Unknown syntax error")
            yield ModelSyntaxError(code=f"{resource_type}-SYNTAX-ERROR", message=message)

    def _local_validation(self, module: Module) -> None:
        """Local validations are post-syntax validations executed"""
        RulesOrchestrator().run(module)

    def _cdf_dependency_validation(self, build_folder: BuildFolder, client: ToolkitClient | None) -> None:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""

        dependencies_by_built_module = build_folder.cdf_dependencies_by_built_module

        if client:
            for built_module, dependencies_by_file in dependencies_by_built_module.items():
                for file, dependencies_by_crud in dependencies_by_file.items():
                    for crud_cls, dependencies in dependencies_by_crud.items():
                        crud = crud_cls(client=client, build_dir=build_folder.path)
                        response = {resource.as_id() for resource in crud.retrieve(list(dependencies))}

                        if missing := dependencies - response:
                            for m in missing:
                                built_module.insights.append(
                                    ConsistencyError(
                                        code="MISSING-DEPENDENCY",
                                        message=(
                                            f"{crud.kind} '{m}' referenced in file '{file}' "
                                            "does not exist locally neither in CDF."
                                        ),
                                        fix="Make sure the resource exists in CDF or remove the reference to it.",
                                    )
                                )
        # Todo: Add recommendation if client missing and there dependencies for the user to supply a
        #   client.

    def _global_validation(self, build_folder: BuildFolder, client: ToolkitClient | None) -> None:
        """This validation is performed per resource type and not per individual resource and against CDF
        for all modules. This validation will leverage external plugins such as NEAT.
        """

        # Can be parallelized if needed
        for built_module in build_folder.built_modules:
            if not built_module.files_built:
                continue

            if files_by_resource_type := built_module.resource_by_type_by_kind.get(DataModelCRUD.folder_name):
                if NeatPlugin.installed() and client and DataModelCRUD.kind in files_by_resource_type:
                    neat = NeatPlugin(client)
                    for data_model_file in files_by_resource_type[DataModelCRUD.kind]:
                        for insight in neat.validate(data_model_file.parent, data_model_file):
                            if insight not in built_module.insights:
                                built_module.insights.append(insight)

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

        insight_file_content = build_folder.insights.to_csv()
        if insight_file_content.strip():
            safe_write(insight_file, insight_file_content)

        lineage_file = build_folder.path / BuildLineage.filename
        lineage_file.parent.mkdir(parents=True, exist_ok=True)
        lineage = BuildLineage.from_build_parameters_and_results(
            parameters, build_folder, timestamp, duration
        ).to_yaml()
        safe_write(lineage_file, lineage)
