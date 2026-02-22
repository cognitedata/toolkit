import os
import sys
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path

from pydantic import JsonValue, ValidationError
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2._module_source_parser import ModuleSourceParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildFolder,
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
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ModelSyntaxError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._plugins import NeatPlugin
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ModelSyntaxError, Recommendation
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    BuildVariable,
    FailedReadResource,
    ReadResource,
    SuccessfulReadResource,
)
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, MODULES
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME, ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.rules import RulesOrchestrator
from cognite_toolkit._cdf_tk.utils import read_yaml_file, safe_write
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils import calculate_hash, humanize_collection, safe_write
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, relative_to_if_possible, safe_read, yaml_safe_dump
from cognite_toolkit._cdf_tk.validation import humanize_validation_error


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuildFolder:
        console = client.console if client else Console()

        self._validate_build_parameters(parameters, console, sys.argv)
        build_files = self._read_file_system(parameters)
        module_sources = self._parse_module_sources(build_files)

        build_folder = self._build_modules(module_sources, parameters.build_dir)

        # Todo: Some mixpanel tracking.
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        self._global_validation(build_folder, client)

        self._write_results(build_folder)
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

    def _build_modules(
        self, module_sources: Sequence[ModuleSource], build_dir: Path, max_workers: int = 1
    ) -> BuildFolder:
        folder: BuildFolder = BuildFolder(path=build_dir)
        for source in module_sources:
            # Inside this loop, do not raise exceptions.
            module = self._import_module(source)  # Syntax validation

            # init built_module
            built_module = BuiltModule(source=module.source)

            if module.is_success:
                self._local_validation(module)
                built_module.built_files = self._export_module(module, build_dir)

            built_module.insights.extend(module.insights)
            folder.built_modules.append(built_module)

        return folder

    def _import_module(self, source: ModuleSource) -> Module:
        resources: list[ReadResource] = []
        for resource_folder, resource_files in source.resource_files_by_folder.items():
            crud_classes = RESOURCE_CRUD_BY_FOLDER_NAME.get(resource_folder)
            if not crud_classes:
                # This is handled in the module parsing phase.
                continue
            class_by_kind = {crud_class.kind: crud_class for crud_class in crud_classes}
            for resource_file in resource_files:
                if "." not in resource_file.stem:
                    # Todo: Discussion error or silent ignore.
                    #   Reason for error is in the case were the user do not set a kind and intends to.
                    #   Reason for silently ignore is that the user for example has a YAML file as part of their
                    #   function code, and it is not meant to be a resource file.
                    continue
                kind = resource_file.stem.rsplit(".", maxsplit=1)[-1]
                crud_class = class_by_kind.get(kind)
                if not crud_class:
                    resources.append(
                        self._create_failed_read_resource_for_invalid_kind(
                            resource_file, kind, resource_folder, class_by_kind.keys()
                        )
                    )
                    continue
                content_or_error = self._read_resource_file(resource_file)
                if isinstance(content_or_error, ModelSyntaxError):
                    resources.append(FailedReadResource(source_path=resource_file, errors=[content_or_error]))
                    continue
                read_resources = self._parse_resource_file(content_or_error, crud_class.yaml_file, source.variables)
                resources.extend(read_resources)

        return Module(source=source, resources=resources)

    def _create_failed_read_resource_for_invalid_kind(
        self, resource_file: Path, kind: str, resource_folder: str, available_kinds: Iterable[str]
    ) -> FailedReadResource:
        return FailedReadResource(
            source_path=resource_file,
            errors=[
                ModelSyntaxError(
                    code="UNKNOWN-RESOURCE-KIND",
                    message=f"Resource file '{resource_file.as_posix()!r}' has unknown resource kind '{kind}' for folder '{resource_folder}'",
                    fix=f"Make sure the file name ends with a known resource kind for the folder. Expected kinds for folder '{resource_folder}' are: {humanize_collection(available_kinds)}",
                )
            ],
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

    def _parse_resource_file(
        self, file_content: str, io: type[ResourceCRUD], variables: list[BuildVariable], file_path: Path
    ) -> list[ReadResource]:
        """Parses a resource file into one or more Toolkit resources.

        1. Reads the YAML file and substitutes variables if needed.
        2. Variable substitution.
        3. Parse the YAML content
        4. Validate the content against the syntax of the YAML model.


        Args:
            file_content: The content of the resource YAML file.
            crud_yaml_model: The Pydantic model class that the YAML file should conform to.
            variables: The variables to substitute in the YAML content.

        Returns:
            A tuple containing the list of parsed ToolkitResource objects and a list of ModelSyntaxError
            objects if there were any syntax errors during parsing.
        """
        file_hash = calculate_hash(file_content, shorten=True)
        results: list[ReadResource] = []
        if variables:
            substituted_content = self._substitute_variables_in_content(file_content, variables)
        else:
            substituted_content = file_content

        try:
            parsed = read_yaml_content(substituted_content)
        except Exception as e:
            # Todo Look for variables not replaced in the content and add fix suggestion to the error.
            results.append(
                FailedReadResource(
                    source_path=file_path,
                    errors=[
                        ModelSyntaxError(
                            code="YAML_PARSE_ERROR",
                            message=f"Failed to parse YAML content: {e!s}",
                            fix="Make sure the YAML content is valid.",
                        )
                    ],
                )
            )
            return results

        listed_resources = parsed if isinstance(parsed, list) else [parsed]
        for resource_dict in listed_resources:
            try:
                resource = io.yaml_cls.model_validate(resource_dict, extra="forbid")
            except ValidationError as forbid_errors:
                try:
                    # Fallback to handle unknown fields.
                    resource = io.yaml_cls.model_validate(resource_dict, extra="ignore")
                except ValidationError:
                    # It is still failing, so we have syntax errors that we want to report.
                    results.append(
                        FailedReadResource(
                            source_path=file_path,
                            errors=[
                                ModelSyntaxError(
                                    code="SYNTAX_ERROR",
                                    message=message,
                                )
                                for message in humanize_validation_error(forbid_errors)
                            ],
                        )
                    )
                else:
                    # Fallback succeeded, so we have insights to report about the unknown fields.
                    results.append(
                        SuccessfulReadResource(
                            source_path=file_path,
                            source_hash=file_hash,
                            resource_type=ResourceType(resource_folder=io.folder_name, kind=io.kind),
                            resource=resource,
                            insights=[
                                Recommendation(
                                    code="UNKNOWN_FIELDS",
                                    message=message,
                                )
                                for message in humanize_validation_error(forbid_errors)
                            ],
                        )
                    )
            else:
                results.append(
                    SuccessfulReadResource(
                        source_path=file_path,
                        source_hash=file_hash,
                        resource_type=ResourceType(resource_folder=io.folder_name, kind=io.kind),
                        resource=resource,
                    )
                )
        return results

    def _substitute_variables_in_content(self, content: str, variables: list[BuildVariable]) -> str:
        raise NotImplementedError()

    def _export_module(self, module: Module, build_dir: Path) -> list[Path]:
        build_dir.mkdir(parents=True, exist_ok=True)

        built_files: list[Path] = []
        for resource_type, resources in module.resources_by_type.items():
            folder = build_dir / resource_type.resource_folder
            folder.mkdir(parents=True, exist_ok=True)
            for index, resource in enumerate(resources, start=1):
                resource_file = folder / f"resource_{index}.{resource_type.kind}.yaml"
                # Todo Move into Toolkit resource.
                safe_write(resource_file, yaml_safe_dump(resource.model_dump(by_alias=True, exclude_unset=True)))
                built_files.append(resource_file)

        # Todo: Store source path, source hash, ID, and so on for build_linage
        return built_files

    @classmethod
    def _create_syntax_errors(cls, resource_type: ResourceType, error: ValidationError) -> Iterable[ModelSyntaxError]:
        # TODO: should be extended with humanizing of errors, this is a quick solution.

        for error_details in error.errors(include_input=True, include_url=False):
            message = error_details.get("msg", "Unknown syntax error")
            yield ModelSyntaxError(code=f"{resource_type}-SYNTAX-ERROR", message=message)

    def _local_validation(self, module: Module) -> None:
        """Local validations are post-syntax validations executed"""
        RulesOrchestrator().run(module)

    def _global_validation(self, build_folder: BuildFolder, client: ToolkitClient | None) -> None:
        """This validation is performed per resource type and not per individual resource and against CDF
        for all modules. This validation will leverage external plugins such as NEAT.
        """

        # Can be parallelized if needed
        for built_module in build_folder.built_modules:
            if not built_module.is_success:
                continue

            if files_by_resource_type := built_module.resource_by_type.get(DataModelCRUD.folder_name):
                if NeatPlugin.installed() and client and DataModelCRUD.kind in files_by_resource_type:
                    neat = NeatPlugin(client)
                    for data_model_file in files_by_resource_type[DataModelCRUD.kind]:
                        for insight in neat.validate(data_model_file.parent, data_model_file):
                            if insight not in built_module.insights:
                                built_module.insights.append(insight)

    def _write_results(self, build_folder: BuildFolder) -> None:
        return None
