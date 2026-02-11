from collections import defaultdict
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2._modules_parser import ModulesParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildParameters,
    BuiltModule,
    BuiltResult,
    InsightList,
    Module,
    ModuleList,
    ModuleResult,
    ReadModule,
    ResourceType,
)
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.utils import read_yaml_file, safe_write
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


class BuildV2Command(ToolkitCommand):
    def build(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuiltResult:
        self._validate_user_input(parameters)
        modules = self.find_modules(parameters)

        results = self._build_and_validate_modules(modules, parameters.build_dir)

        # Todo: Some mixpanel tracking.
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        global_insights = self.global_validation(results, client)

        built_results = BuiltResult(module_results=results, global_insights=global_insights)

        self.write_results(parameters.build_dir, built_results)
        return built_results

    def _validate_user_input(self, parameters: BuildParameters) -> None:
        if not parameters.organization_dir.exists():
            raise ToolkitNotADirectoryError(
                f"Organization directory '{parameters.organization_dir.as_posix()}' not found"
            )
        # Todo Clean build directory if it exists, or at least check if it's empty.

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
