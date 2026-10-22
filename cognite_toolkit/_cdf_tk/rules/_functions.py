from collections.abc import Iterable
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionLimits
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, FunctionsAcl
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO, ResourceType
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet, with_position
from cognite_toolkit._cdf_tk.utils import humanize_collection, validate_requirements_with_pip
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.yaml_classes.functions import FunctionsYAML


class FunctionRuleSet(ToolkitGlobalRuleSet):
    CODE_PREFIX = "FUNCTION"
    DISPLAY_NAME = "Functions"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "Function limits validation requires a client. "
                    "Provide client credentials to validate function CPU and MEMORY limits."
                    "Will only validate the requirements.txt."
                ),
            )

        return RuleSetStatus(
            code="ready",
            message="Will validate function limits, runtime, and requirements.txt.",
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        functions: list[BuiltResource] = []
        function_type = ResourceType(resource_folder=FunctionIO.folder_name, kind=FunctionIO.kind)
        for module in self.modules:
            for resource in module.resources:
                if not resource.can_verify:
                    # We do not do further validation if there are syntax errors.
                    continue
                if resource.type == function_type:
                    functions.append(resource)

        if not functions:
            return

        is_function_service_available = True
        try:
            if self.client:
                for insight in self._validate_function_service(self.client, functions[0].source_path):
                    is_function_service_available = False
                    yield insight
        except Exception as e:
            yield InternalValidatorException(
                message=f"Function service validation failed: {e}",
                source="shared",
            )
        limits: FunctionLimits | None = None
        if self.client and is_function_service_available:
            try:
                limits = self.client.functions.limits()
            except Exception as e:
                yield InternalValidatorException(
                    message=f"Function limits retrieval failed: {e}",
                    source="shared",
                )

        for resource in functions:
            try:
                yield from self._validate_function(resource, limits)
            except Exception as e:
                yield InternalValidatorException(
                    message=f"Function limits validator failed for function definition {resource.build_path.name!r}: {e}",
                    source=str(resource.identifier),
                )

    @classmethod
    def _validate_function_service(cls, client: ToolkitClient, first_function: Path) -> Iterable[Insight]:
        if missing := client.token.verify_acls([FunctionsAcl(actions=["READ"], scope=AllScope())]):
            yield BuildError(
                message=(
                    f"Function limits validation requires READ access to Functions. "
                    f"The following ACLs are missing: {humanize_collection(missing)}."
                ),
                code="MISSING-READ-ACCESS",
                fix="Ensure that the provided token has READ access to Functions.",
                source_file=first_function,
            )
            return

        status = client.functions.status()
        if status.status != "activated":
            yield BuildError(
                message=(
                    f"Function limits validation requires Cognite Functions to be activated. "
                    f"Current status: {status.status}."
                ),
                code="INVALID-VALUE",
                fix="Activate Cognite Functions for the project. For example, by running `cdf auth verify`.",
                source_file=first_function,
            )

    @classmethod
    def _validate_function(cls, resource: BuiltResource, limits: FunctionLimits | None) -> Iterable[Insight]:
        """Validate function definitions against CDF project limits.

        Args:
            function_file: Path to the function YAML file.

        Yields:
            Errors for any violations of function limits.
        """
        # Parse function_file (YAML) to dict/list, then create FunctionsYAML objects to validate and extract definitions
        raw_data = read_yaml_file(resource.build_path, expected_output="dict")

        # Should already be validated.
        function_def = FunctionsYAML.model_validate(raw_data)

        if limits:
            yield from cls._validate_limits(limits, function_def, resource.source_path)

        function_folder = FunctionIO.get_function_code_implicitly(resource.source_path, function_def.as_id())
        if function_folder.is_dir() and (requirement_txt := next(function_folder.rglob("requirements.txt"), None)):
            pip_result = validate_requirements_with_pip(
                requirement_txt, function_def.index_url, function_def.extra_index_urls
            )
            if not pip_result.success:
                yield BuildError(
                    message=pip_result.create_message("Function", function_def.external_id),
                    code="INVALID-REQUIREMENTS",
                    fix="Ensure that requirements.txt is valid.",
                    source_file=resource.source_path,
                )

    @classmethod
    def _validate_limits(
        cls, limits: FunctionLimits, function_def: FunctionsYAML, source_path: Path
    ) -> Iterable[Insight]:
        # Ensure we always work with a list of function definitions
        # Validate CPU cores
        if function_def.cpu is not None and limits:
            if function_def.cpu < limits.cpu_cores.min or function_def.cpu > limits.cpu_cores.max:
                yield with_position(
                    BuildError(
                        message=(
                            f"Function '{function_def.external_id}' CPU cores ({function_def.cpu}) "
                            f"must be between {limits.cpu_cores.min} and {limits.cpu_cores.max}."
                        ),
                        code="EXCEEDED-LIMIT",
                        fix=f"Ensure that CPU cores is between {limits.cpu_cores.min} and {limits.cpu_cores.max}.",
                        source_file=source_path,
                    ),
                    keys=["cpu"],
                )

        # Validate memory
        if function_def.memory is not None and limits:
            if function_def.memory < limits.memory_gb.min or function_def.memory > limits.memory_gb.max:
                yield with_position(
                    BuildError(
                        message=(
                            f"Function '{function_def.external_id}' memory ({function_def.memory} GB) "
                            f"must be between {limits.memory_gb.min} and {limits.memory_gb.max} GB."
                        ),
                        code="EXCEEDED-LIMIT",
                        fix=f"Ensure that memory is between {limits.memory_gb.min} and {limits.memory_gb.max} GB.",
                        source_file=source_path,
                    ),
                    keys=["memory"],
                )

        # Validate runtime
        if function_def.runtime is not None and limits is not None:
            if function_def.runtime not in limits.runtimes:
                quoted_runtimes = humanize_collection([f"{runtime!r}" for runtime in limits.runtimes])
                yield BuildError(
                    message=(
                        f"Function '{function_def.external_id}' runtime {function_def.runtime!r} is not "
                        f"available in this CDF project. "
                        f"Available runtimes: {quoted_runtimes}."
                    ),
                    code="INVALID-VALUE",
                    fix=f"Use one of the available runtimes: {quoted_runtimes}.",
                    source_file=source_path,
                )
