"""Build command acts as an orchestrator for the entire build process, which includes:

- Selection of Modules to be built
- Discovery of Modules and Modules' Resources (aka files)
- Reading of Modules' Resources (aka files), which includes variable substitution and general syntax (generic format) validation
- Specific syntax validation for each resource type (e.g. whether or not YAML files container necessary fields, plus internal Rules)
- Global validation against CDF (typically performed by external plugins, such as NEAT for data modeling)
- Compiling of Modules' Resources into a format that can be used for deployment
- Write out of the build process linage and insights
"""


from calendar import c
from dataclasses import dataclass
from typing import Any
from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.data_classes._built_modules import BuiltModuleList

from ._insights import InsightList


@dataclass
class BuildResults:
    """Results from building modules in the toolkit.

    This class serves as a container for the outputs of a build operation, capturing
    both the successfully built modules and any insights or observations generated
    during the build process.

    Attributes:
        built_modules (BuiltModuleList): A list of modules that were successfully built
            during the build process.
        insights (InsightList): A collection of insights (syntax errors, consistency errors, consistency warnings and
            recommendation) that are generated during the build process, which may include optimization
            suggestions, potential issues, or other relevant information.
    """
    built_modules: BuiltModuleList
    insights: InsightList

class ModuleReadConfig: ...
class ReadModule: ...
class ValidateLocal: ...
class ValidateGlobal: ...


class BuildCommand(ToolkitCommand):
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
        client: ToolkitClient | None = None,
    ) -> None:
        super().__init__(print_warning, skip_tracking, silent, client)
        self.insights = InsightList()
        self.client = client


    def build(self, *args: Any) -> BuildResults:

        # takes config.[dev|test|prod].yaml and generates build parameters

        parameters = BuildParameters.load(...)

        # find modules and their resources based on the build parameters

        read_cfg = ModuleReadConfig(parameters=parameters).find()

        self.insights.append(read_cfg.insights)

        # read the content of the modules' resources, which includes:
        # 1. variable substitution
        # 2. general syntax (generic format) validation

        read_results = ReadModule(read_cfg).read()

        self.insights.append(read_results.insights)

        # compile API friendly objects from the read results

        compilation_results = read_results.compile()

        # run validation local
        self.insights.append(ValidateLocal(read_results).validate())

        # run validation against CDF, which typically requires external plugins, such as NEAT for data modeling
        if self.client is not None:
            self.insights.append(ValidateGlobal(compilation_results, self.client).validate())



        return BuildResults(
            built_modules=compilation_results.built_modules,
            insights=self.insights,)
