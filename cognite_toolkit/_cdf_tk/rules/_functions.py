from collections.abc import Iterable
from functools import cached_property

from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionLimits
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, FailedValidation
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet
from cognite_toolkit._cdf_tk.utils import validate_requirements_with_pip
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.yaml_classes.functions import FunctionsYAML


class FunctionRules(ToolkitGlobalRuleSet):
    CODE_PREFIX = "FUNCTION"
    DISPLAY_NAME = "Functions checks"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "Function limits validation requires a client. "
                    "Provide client credentials to validate function CPU and MEMORY limits."
                    "Will only validate the requirement txt."
                ),
            )

        return RuleSetStatus(
            code="ready",
            message="Will validate function limits and requirement txt.",
        )

    def validate(self) -> Iterable[ConsistencyError | FailedValidation]:
        function_type = ResourceType(resource_folder=FunctionIO.folder_name, kind=FunctionIO.kind)
        for module in self.modules:
            for resource in module.resources:
                if not resource.can_verify:
                    # We do not do further validation if there are syntax errors.
                    continue
                if resource.type == function_type:
                    try:
                        yield from self._validate_function(resource)
                    except Exception as e:
                        yield FailedValidation(
                            message=f"Function limits validation failed for function definition {resource.build_path.name!r}: {e}",
                            source=str(resource.identifier),
                        )

    def _validate_function(self, resource: BuiltResource) -> Iterable[ConsistencyError]:
        """Validate function definitions against CDF project limits.

        Args:
            function_file: Path to the function YAML file.

        Yields:
            ConsistencyError for any violations of function limits.
        """
        # Parse function_file (YAML) to dict/list, then create FunctionsYAML objects to validate and extract definitions
        raw_data = read_yaml_file(resource.build_path, expected_output="dict")

        # Ensure we always work with a list of function definitions
        limits = self.limits

        # Validate against schema
        function_def = FunctionsYAML.model_validate(raw_data)

        # Validate CPU cores
        if function_def.cpu is not None and limits:
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
        if function_def.memory is not None and limits:
            if function_def.memory < limits.memory_gb.min or function_def.memory > limits.memory_gb.max:
                yield ConsistencyError(
                    message=(
                        f"Function '{function_def.external_id}' memory ({function_def.memory} GB) "
                        f"must be between {limits.memory_gb.min} and {limits.memory_gb.max} GB."
                    ),
                    code=f"{self.CODE_PREFIX}-MEMORY",
                    fix=f"Ensure that memory is between {limits.memory_gb.min} and {limits.memory_gb.max} GB.",
                )

        function_folder = FunctionIO.get_function_code_implicitly(resource.source_path, function_def.as_id())
        if function_folder.is_dir() and (requirement_txt := next(function_folder.rglob("requirements.txt"), None)):
            pip_result = validate_requirements_with_pip(
                requirement_txt, function_def.index_url, function_def.extra_index_urls
            )
            if not pip_result.success:
                yield ConsistencyError(
                    message=pip_result.create_message("Function", function_def.external_id),
                    code=f"{self.CODE_PREFIX}-REQUIREMENTS-TXT",
                    fix="Ensure that requirements.txt is valid.",
                )

    @cached_property
    def limits(self) -> FunctionLimits | None:
        if not self.client:
            return None
        return self.client.tool.functions.limits()
