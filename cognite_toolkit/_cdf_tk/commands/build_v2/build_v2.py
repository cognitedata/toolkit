from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2.build_parameters import BuildParameters
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuiltModule,
    BuiltResult,
    InsightList,
    ModuleList,
    ModuleResult,
    ReadModule,
    SelectedModule,
)


class BuildV2Command(ToolkitCommand):
    def build_folder(self, parameters: BuildParameters, client: ToolkitClient | None = None) -> BuiltResult:
        modules = self.find_modules(parameters)

        results = self._build_and_validate_modules(modules)

        # Todo: Some mixpanel tracking.
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        global_insights = self.global_validation(results, client)

        built_results = BuiltResult(module_results=results, global_insights=global_insights)

        self.write_results(parameters.build_dir, built_results)
        return built_results

    def _build_and_validate_modules(self, modules: ModuleList, max_workers: int = 1) -> list[ModuleResult]:
        results: list[ModuleResult] = []
        for module in modules:
            # Inside this loop, do not raise exceptions.
            read_module = self.read_module(module)  # Syntax validation

            built_module: BuiltModule | None = None
            validation_insights: InsightList | None = None
            if read_module.is_success:
                built_module = self.build_module(module)
                validation_insights = self.validate_module(module)

            results.append(self.compile(module, read_module, built_module, validation_insights))
        return results

    def find_modules(self, parameters: BuildParameters) -> ModuleList:
        raise NotImplementedError

    def read_module(self, module: SelectedModule) -> ReadModule:
        raise NotImplementedError

    def validate_module(self, module: SelectedModule) -> InsightList:
        raise NotImplementedError

    def build_module(self, module: SelectedModule) -> BuiltModule:
        raise NotImplementedError

    def compile(
        self,
        module: SelectedModule,
        read_result: ReadModule,
        built_module: BuiltModule | None,
        validation_insights: InsightList | None,
    ) -> ModuleResult:
        # ModuleResults should not contain the resource in memory, just their ID,
        # paths, and on.
        raise NotImplementedError

    def global_validation(self, modules: list[ModuleResult], client: ToolkitClient | None) -> InsightList:
        raise NotImplementedError

    def write_results(self, build_dir: Path, built_results: BuiltResult) -> None:
        raise NotImplementedError()
