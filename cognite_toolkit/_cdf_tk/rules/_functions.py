from collections.abc import Iterable
from functools import cached_property
from pathlib import Path

from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionLimits
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import FailedValidation
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.yaml_classes.functions import FunctionsYAML

from ._base import ToolkitGlobalRulSet


class FunctionLimitsRule(ToolkitGlobalRulSet):
    CODE_PREFIX = "FUNCTION"
    DISPLAY_NAME = "Function limits"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="unavailable",
                message=(
                    "Function limits validation requires a client. "
                    "Provide client credentials to use Neat for validation."
                ),
            )

        return RuleSetStatus(
            code="ready",
            message="Will validate function definitions against CDF Project limits.",
        )

    def validate(self) -> Iterable[ConsistencyError | FailedValidation]:
        function_type = ResourceType(resource_folder=FunctionIO.folder_name, kind=FunctionIO.kind)
        for module in self.modules:
            for resource in module.resources:
                if resource.type == function_type:
                    try:
                        yield from self._validate_function(resource.build_path)
                    except Exception as e:
                        yield FailedValidation(
                            message=f"Function limits validation failed for function definition {resource.build_path.name!r}: {e}",
                            source=str(resource.identifier),
                        )

    def _validate_function(self, function_file: Path) -> Iterable[ConsistencyError]:
        """Validate function definitions against CDF project limits.

        Args:
            function_file: Path to the function YAML file.

        Yields:
            ConsistencyError for any violations of function limits.
        """
        # Parse function_file (YAML) to dict/list, then create FunctionsYAML objects to validate and extract definitions
        raw_data = read_yaml_file(function_file, expected_output="dict")

        # Ensure we always work with a list of function definitions
        function_defs = [raw_data] if isinstance(raw_data, dict) else raw_data

        limits = self.limits

        for function_def_data in function_defs:
            # Validate against schema
            function_def = FunctionsYAML.model_validate(function_def_data)

            # Validate CPU cores
            if function_def.cpu is not None:
                if function_def.cpu < limits.cpu_cores.min or function_def.cpu > limits.cpu_cores.max:
                    yield ConsistencyError(
                        message=(
                            f"Function '{function_def.external_id}' CPU cores ({function_def.cpu}) "
                            f"must be between {limits.cpu_cores.min} and {limits.cpu_cores.max}."
                        ),
                        code=f"{self.CODE_PREFIX}-CPU",
                        fix=f"Ensure that CPU cores is between {limits.cpu_cores.min} and {limits.cpu_cores.max}.",
                    )

            # Validate memory
            if function_def.memory is not None:
                if function_def.memory < limits.memory_gb.min or function_def.memory > limits.memory_gb.max:
                    yield ConsistencyError(
                        message=(
                            f"Function '{function_def.external_id}' memory ({function_def.memory} GB) "
                            f"must be between {limits.memory_gb.min} and {limits.memory_gb.max} GB."
                        ),
                        code=f"{self.CODE_PREFIX}-MEMORY",
                        fix=f"Ensure that memory is between {limits.memory_gb.min} and {limits.memory_gb.max} GB.",
                    )

    @cached_property
    def limits(self) -> FunctionLimits:
        if not self.client:
            raise ValueError("Client is required to fetch function limits.")
        return self.client.tool.functions.limits()
