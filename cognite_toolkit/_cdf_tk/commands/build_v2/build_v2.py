import os
import sys
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from pydantic import JsonValue, ValidationError
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2._modules_parser import ModulesParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildFolder,
    BuildParameters,
    BuiltModule,
    ConfigYAML,
    InsightList,
    Module,
    ModuleSource,
    ModuleSources,
    ParseInput,
    RelativeDirPath,
    ResourceType,
    ValidationType,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ModelSyntaxError, Recommendation
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, MODULES
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.utils import read_yaml_file, safe_write
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible, yaml_safe_dump
from cognite_toolkit._cdf_tk.validation import humanize_validation_error


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuildFolder:
        console = client.console if client else Console()
        self.validate_build_parameters(parameters, console, sys.argv)
        parse_inputs = self.read_parameters(parameters)
        module_sources = self.parse_module_sources(parse_inputs, parameters.organization_dir)

        build_folder = self._create_build_folder(module_sources, parameters.build_dir)

        # Todo: Some mixpanel tracking.
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        build_folder.insights.extend(self.global_validation(build_folder, client))

        self.write_results(build_folder)
        return build_folder

    @classmethod
    def validate_build_parameters(cls, parameters: BuildParameters, console: Console, user_args: list[str]) -> None:
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

    def _create_build_folder(self, module_sources: ModuleSources, build_dir: Path, max_workers: int = 1) -> BuildFolder:
        folder: BuildFolder = BuildFolder()

        for source in module_sources:
            # Inside this loop, do not raise exceptions.
            module = self.import_module(source)  # Syntax validation

            folder.insights.extend(module.insights)

            built_module: BuiltModule | None = None
            validation_insights: InsightList | None = None
            if module.is_success:
                validation_insights = self.validate_module(module)
                built_module = self.export_module(module, build_dir)

                folder.insights.extend(validation_insights)
                folder.add_build_files(built_module.built_files)
        return folder

    @classmethod
    def read_parameters(cls, parameters: BuildParameters) -> ParseInput:
        selected: set[RelativeDirPath | str] = {
            parameters.modules_directory.relative_to(parameters.organization_dir)
        }  # Default to everything under modules.
        variables: dict[str, JsonValue] = {}
        cdf_project: str = os.environ.get("CDF_PROJECT", "UNKNOWN")
        validation_type: ValidationType = "prod"
        if parameters.user_selected_modules:
            selected = cls._parse_user_selection(parameters.user_selected_modules, parameters.organization_dir)

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
                selected = cls._parse_user_selection(config.environment.selected, parameters.organization_dir)
            variables = config.variables or {}
            cdf_project = config.environment.project
            validation_type = config.environment.validation_type

        # Todo optimize by only searching for yaml files in the selected modules paths if selection is provided.
        yaml_files = list(parameters.modules_directory.rglob("*.y*ml"))
        return ParseInput(
            yaml_files=yaml_files,
            selected_modules=selected,
            variables=variables,
            validation_type=validation_type,
            cdf_project=cdf_project,
        )

    @classmethod
    def _parse_user_selection(
        cls, user_selected_modules: list[str], organization_dir: Path
    ) -> set[RelativeDirPath | str]:
        selected: set[RelativeDirPath | str] = set()
        errors: list[str] = []
        for item in user_selected_modules:
            if "/" not in item:
                # Module name provided
                selected.add(item)
                continue

            item_path = Path(item)
            if not (organization_dir / item_path).exists():
                errors.append(
                    f"Selected module path {item_path.as_posix()!r} does not exist under the organization directory"
                )
                continue
            if not item_path.is_dir():
                errors.append(f"Selected module path {item_path.as_posix()!r} is not a directory")
                continue
            if item_path.is_absolute():
                errors.append(
                    f"Selected module path {item_path.as_posix()!r} should be relative to the organization directory"
                )
                continue
            selected.add(item_path)
        if errors:
            raise ToolkitValueError("Invalid module selection:\n" + "\n".join(f"- {error}" for error in errors))
        return selected

    def parse_module_sources(self, parse_inputs: ParseInput, organization_dir: Path) -> ModuleSources:
        # Parse the variables.
        module_paths = ModulesParser(organization_dir=organization_dir).parse()
        return ModuleSources(
            [
                ModuleSource(path=module_path, id=module_path.relative_to(organization_dir))
                for module_path in module_paths
            ]
        )

    def import_module(self, module_source: ModuleSource) -> Module:
        insights: InsightList = InsightList()
        resource_folder_paths = [
            resource_path for resource_path in module_source.path.iterdir() if resource_path.is_dir()
        ]

        resource_by_type: dict[ResourceType, list[ToolkitResource]] = defaultdict(list)
        for resource_folder_path in resource_folder_paths:
            crud_classes = RESOURCE_CRUD_BY_FOLDER_NAME.get(resource_folder_path.name)
            if not crud_classes:
                # This is handled in the module parsing phase.
                continue
            for crud_class in crud_classes:
                resource_type = ResourceType(resource_folder=resource_folder_path.name, kind=crud_class.kind)
                resource_files = list(resource_folder_path.rglob(f"*.{crud_class.kind}.y*ml"))
                for resource_file in resource_files:
                    # Todo: Create a classmethod for ToolkitResource
                    # Todo; Handle lists of resources in a single file
                    try:
                        resource = crud_class.yaml_cls.model_validate(read_yaml_file(resource_file))
                        resource_by_type[resource_type].append(resource)
                    except ValidationError as e:
                        insights.extend(self._create_syntax_errors(resource_type, e))

        return Module(source=module_source, resources_by_type=resource_by_type, insights=insights)

    def validate_module(self, module: Module) -> InsightList:
        return InsightList()

    def export_module(self, module: Module, build_dir: Path) -> BuiltModule:
        build_dir.mkdir(parents=True, exist_ok=True)

        built_module = BuiltModule(source=module.source)
        for resource_type, resources in module.resources_by_type.items():
            folder = build_dir / resource_type.resource_folder
            folder.mkdir(parents=True, exist_ok=True)
            for index, resource in enumerate(resources, start=1):
                resource_file = folder / f"resource_{index}.{resource_type.kind}.yaml"
                # Todo Move into Toolkit resource.
                safe_write(resource_file, yaml_safe_dump(resource.model_dump(by_alias=True, exclude_unset=True)))
                built_module.built_files.append(resource_file)
        # Todo: Store source path, source hash, ID, and so on for build_linage
        return built_module

    def global_validation(self, build_folder: BuildFolder, client: ToolkitClient | None) -> InsightList:

        # can be parallelized
        insights = InsightList()

        if files_by_resource_type := build_folder.resource_by_type.get(DataModelCRUD.folder_name):
            insights.extend(self._validate_with_neat(files_by_resource_type, client))

        return insights

    def write_results(self, build_folder: BuildFolder) -> None:
        return None

    @classmethod
    def _create_syntax_errors(cls, resource_type: ResourceType, error: ValidationError) -> Iterable[ModelSyntaxError]:
        # TODO: should be extended with humanizing of errors, this is a quick solution.

        for error_details in error.errors(include_input=True, include_url=False):
            message = error_details.get("msg", "Unknown syntax error")
            yield ModelSyntaxError(code=f"{resource_type}-SYNTAX-ERROR", message=message)

    def _validate_with_neat(
        self, files_by_resource_type: dict[str, list[Path]], client: ToolkitClient | None
    ) -> InsightList:
        """Placeholder for NEAT validation."""

        insights = InsightList()

        try:
            from cognite.neat._issues import Recommendation as NeatRecommendation

            neat_insight = NeatRecommendation(
                message="Good job! You are using Neat!",
                code="NEAT-USER",
            )
            insights.append(Recommendation.model_validate(neat_insight.model_dump()))
        except ImportError:
            local_insight = Recommendation(
                message="It is always a good idea to use Neat!",
                code="NEAT-EVANGELISM",
                fix="pip install cognite-toolkit[v08]",
            )
            insights.append(local_insight)

        return insights
