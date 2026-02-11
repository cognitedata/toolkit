import sys
from collections import defaultdict
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2._modules_parser import ModulesParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildParameters,
    BuiltModule,
    BuiltResult,
    ConfigYAML,
    InsightList,
    Module,
    ModuleList,
    ModuleResult,
    ReadModule,
    ResourceType,
)
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, MODULES
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitNotADirectoryError
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.utils import read_yaml_file, safe_write
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible, yaml_safe_dump


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuiltResult:
        console = client.console if client else Console()
        self.validate_build_parameters(parameters, console, sys.argv)
        modules = self.find_modules(parameters)

        results = self._build_and_validate_modules(modules, parameters.build_dir)

        # Todo: Some mixpanel tracking.
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        global_insights = self.global_validation(results, client)

        built_results = BuiltResult(module_results=results, global_insights=global_insights)

        self.write_results(parameters.build_dir, built_results)
        return built_results

    @classmethod
    def validate_build_parameters(cls, parameters: BuildParameters, console: Console, user_args: list[str]) -> None:
        """Checks that the user has the correct folders set up and that the config file (if provided) exists."""

        # Set up the variables
        organization_dir = parameters.organization_dir
        config_yaml_path: Path | None = None
        if parameters.config_yaml_name:
            config_yaml_path = organization_dir / ConfigYAML.get_filename(parameters.config_yaml_name)
            content = f"  ┣ {MODULES}/\n  ┗ {config_yaml_path.name}\n"
        else:
            content = f"  ┗ {MODULES}\n"
        expected_panel = Panel(
            f"Toolkit expects the following structure:\n{organization_dir.as_posix()!r}/\n{content}",
            expand=False,
        )
        module_directory = parameters.organization_dir / MODULES

        # Execute the checks.
        if module_directory.exists() and (config_yaml_path is None or config_yaml_path.exists()):
            # All good.
            return

        console.print(expected_panel)
        if not organization_dir.exists():
            raise ToolkitNotADirectoryError(
                f"Organization directory '{parameters.organization_dir.as_posix()}' not found"
            )
        elif module_directory.exists() and config_yaml_path is not None and not config_yaml_path.exists():
            raise ToolkitFileNotFoundError(f"Config YAML file '{config_yaml_path.as_posix()}' not found")
        elif not module_directory.exists():
            # This is likely the most common error. The user pass in Path.cwd() as the organization directory,
            # but the actual organization directory is a subdirectory.
            candidate_org = next(
                subdir for subdir in organization_dir.iterdir() if subdir.iterdir() and (subdir / MODULES).exists()
            )
            if candidate_org:
                display_path = relative_to_if_possible(candidate_org, Path.cwd())
                suggested_command = cls._create_suggested_command(display_path, user_args)
                console.print(f"{HINT_LEAD_TEXT}: Did you mean to use the command {suggested_command}")
                cdf_toml = CDFToml.load()
                if not cdf_toml.cdf.has_user_set_default_org:
                    print(
                        f"{HINT_LEAD_TEXT} You can specify a 'default_organization_dir = ...' in the 'cdf' section of your "
                        f"'{CDFToml.file_name}' file to avoid using the -o/--organization-dir argument"
                    )
            raise ToolkitNotADirectoryError(
                f"Could not find the modules directory.{module_directory.as_posix()!r} directory."
            )
        else:
            raise NotImplementedError("Unhandled case. Please report this.")

    @classmethod
    def _create_suggested_command(cls, display_path: Path, user_args: list[str]) -> str:
        suggestion = ["cdf"]
        skip_next = False
        found = False
        for arg in user_args[1:]:
            if arg in ("-o", "--organization-dir"):
                suggestion.append(f"{arg} {display_path}")
                skip_next = True
                found = True
                continue
            if skip_next:
                skip_next = False
                continue
            suggestion.append(arg)
        if not found:
            suggestion.append(f"-o {display_path}")
        return f"'{' '.join(suggestion)}'"

    def _build_and_validate_modules(
        self, modules: ModuleList, build_dir: Path, max_workers: int = 1
    ) -> list[ModuleResult]:
        results: list[ModuleResult] = []
        for module in modules:
            # Inside this loop, do not raise exceptions.
            read_module = self.read_module(module)  # Syntax validation

            built_module: BuiltModule | None = None
            validation_insights: InsightList | None = None
            if read_module.is_success:
                built_module = self.build_module(read_module, build_dir)
                validation_insights = self.validate_module(read_module)

            results.append(self.compile(module, read_module, built_module, validation_insights))
        return results

    def find_modules(self, parameters: BuildParameters) -> ModuleList:
        module_paths = ModulesParser(organization_dir=parameters.organization_dir).parse()
        return ModuleList([Module(path=module_path) for module_path in module_paths])

    def read_module(self, module: Module) -> ReadModule:
        resource_folder_paths = [resource_path for resource_path in module.path.iterdir() if resource_path.is_dir()]

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
                    resource = crud_class.yaml_cls.model_validate(read_yaml_file(resource_file))
                    resource_by_type[resource_type].append(resource)

        return ReadModule(resources_by_type=resource_by_type)

    def validate_module(self, module: ReadModule) -> InsightList:
        return InsightList()

    def build_module(self, module: ReadModule, build_dir: Path) -> BuiltModule:
        build_dir.mkdir(parents=True, exist_ok=True)
        for resource_type, resources in module.resources_by_type.items():
            folder = build_dir / resource_type.resource_folder
            folder.mkdir(parents=True, exist_ok=True)
            for index, resource in enumerate(resources, start=1):
                resource_file = folder / f"resource_{index}.{resource_type.kind}.yaml"
                # Todo Move into Toolkit resource.
                safe_write(resource_file, yaml_safe_dump(resource.model_dump(by_alias=True, exclude_unset=True)))
        # Todo: Store source path, source hash, ID, and so on for build_linage
        return BuiltModule()

    def compile(
        self,
        module: Module,
        read_result: ReadModule,
        built_module: BuiltModule | None,
        validation_insights: InsightList | None,
    ) -> ModuleResult:
        # ModuleResults should not contain the resource in memory, just their ID,
        # paths, and on.
        return ModuleResult()

    def global_validation(self, modules: list[ModuleResult], client: ToolkitClient | None) -> InsightList:
        return InsightList()

    def write_results(self, build_dir: Path, built_results: BuiltResult) -> None:
        return None
