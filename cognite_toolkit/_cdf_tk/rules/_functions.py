from collections.abc import Iterable
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionLimits, FunctionRequest
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, FunctionsAcl
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet, quote_identifier, with_position
from cognite_toolkit._cdf_tk.utils import humanize_collection, validate_requirements_with_pip


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
        functions: list[tuple[Path, FunctionRequest]] = []
        crud = FunctionIO(self._load_client())
        for resource in self.resources:
            if not resource.has_valid_syntax or resource.type != FunctionIO.as_resource_type():
                # We do not do further validation if there are syntax errors.
                continue
            if not isinstance(resource.source, Path):
                continue
            try:
                functions.append((resource.source, resource.as_request(crud, is_dry_run=True).request))
            except Exception as e:
                yield InternalValidatorException(
                    message=f"Failed to load function definition {quote_identifier(resource.identifier)}: {e}",
                    source=str(resource.identifier),
                )

        if not functions:
            return

        is_function_service_available = True
        try:
            if self.client:
                for insight in self._validate_function_service(self.client, functions[0][0]):
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

        for source_path, function_def in functions:
            try:
                yield from self._validate_function(source_path, function_def, limits)
            except Exception as e:
                yield InternalValidatorException(
                    message=f"Function limits validator failed for function {function_def.external_id!r}: {e}",
                    source=str(function_def.external_id),
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
    def _validate_function(
        cls, source_path: Path, function_def: FunctionRequest, limits: FunctionLimits | None
    ) -> Iterable[Insight]:
        """Validate function definitions against CDF project limits.

        Args:
            source_path: Path to the function YAML source file.
            function_def: The function definition to validate.
            limits: The function limits of the CDF project, if available.

        Yields:
            Errors for any violations of function limits.
        """
        if limits:
            yield from cls._validate_limits(limits, function_def, source_path)

        function_folder = function_def.function_code
        if (
            function_folder is not None
            and function_folder.is_dir()
            and (requirement_txt := next(function_folder.rglob("requirements.txt"), None))
        ):
            pip_result = validate_requirements_with_pip(
                requirement_txt, function_def.index_url, function_def.extra_index_urls
            )
            if not pip_result.success:
                yield BuildError(
                    message=pip_result.create_message("Function", function_def.external_id or function_def.name),
                    code="INVALID-REQUIREMENTS",
                    fix="Ensure that requirements.txt is valid.",
                    source_file=source_path,
                )

    @classmethod
    def _validate_limits(
        cls, limits: FunctionLimits, function_def: FunctionRequest, source_path: Path
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
                yield with_position(
                    BuildError(
                        message=(
                            f"Function '{function_def.external_id}' runtime {function_def.runtime!r} is not "
                            f"available in this CDF project. "
                            f"Available runtimes: {quoted_runtimes}."
                        ),
                        code="INVALID-VALUE",
                        fix=f"Use one of the available runtimes: {quoted_runtimes}.",
                        source_file=source_path,
                    ),
                    keys=["runtime"],
                )
