import builtins
import re
from collections import UserList
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generic, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator
from pydantic_core.core_schema import FieldValidationInfo

from cognite_toolkit._cdf_tk.client._resource_base import Identifier, T_Identifier, T_RequestResource
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionRequest
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO
from cognite_toolkit._cdf_tk.resource_ios._base_ios import (
    FailedReadExtra,
    ResourceIO,
    ResourceType,
    SuccessExtra,
)
from cognite_toolkit._cdf_tk.utils.file import find_unique_variable_position

from ._config import DeployProtection
from ._insights import (
    BuildError,
    BuildWarning,
    Insight,
    InsightList,
    InternalValidatorException,
    Recommendation,
)
from ._module import BuildVariable, FailedReadYAMLFile, IgnoredFile, ModuleId
from ._types import AbsoluteDirPath, AbsoluteFilePath, RelativeDirPath, RelativeFilePath, ValidationType

UNRESOLVED_VARIABLE_PATTERN = re.compile(r"\{\{.*?\}\}")


def contains_unresolved_variable(value: object) -> bool:
    return UNRESOLVED_VARIABLE_PATTERN.search(str(value)) is not None


class BuildParameters(BaseModel):
    organization_dir: Path
    build_dir: Path = Field(default_factory=lambda: Path.cwd() / "build")
    config_yamls: list[Path] | None = Field(
        default=None,
        description="List of configuration YAML files (typically config.<env>.yaml under the organization directory).",
    )
    user_selected_modules: list[str] | None = Field(
        None,
        description="List of module names or paths to build. If not provided, Toolkit will first attempt to find a config YAML "
        "and the modules specified there. If no config YAML is found, Toolkit will prompt the user to choose the modules"
        "to build",
    )
    verbose: bool = False
    operation: str = Field(
        default="build",
        description="The operation being performed. Used when prompting the user for input. For example, 'Which modules do you want to build?'",
    )

    @field_validator("user_selected_modules", mode="after")
    @classmethod
    def user_selected_modules_not_compatible_with_multiple_config_yamls(
        cls, v: list[str] | None, info: FieldValidationInfo
    ) -> list[str] | None:
        if v and info.data.get("config_yamls") and len(info.data["config_yamls"]) > 1:
            raise ValueError(
                "Cannot specify user_selected_modules when multiple config YAMLs are provided. "
                "Please specify the modules to build in each config YAML instead."
            )
        return v

    @property
    def modules_directory(self) -> Path:
        return self.organization_dir / MODULES

    @property
    def config_file_name(self) -> str:
        return self.config_yamls[0].name if self.config_yamls else ""


class PostBuildParameters(BaseModel):
    insight_format: Literal["csv", "json"] = Field(
        default="csv",
        description="Format for the insights file written to the build directory.",
    )
    write_insights: bool = Field(
        default=True,
        description="Whether to write insights to a file in the build directory.",
    )
    write_lineage: bool = Field(
        default=True,
        description="Whether to write lineage information to a file in the build directory.",
    )
    rules_ignore: set[str] = Field(
        default_factory=set,
        description="Set of rule codes to ignore during validation.",
    )
    rules_enforce: bool = Field(
        default=False,
        description="Whether to enforce rules during validation. If True, any rule violations will cause the build to fail. If False, rule violations will be reported as warnings.",
    )


class ConfigFile(BaseModel):
    selected_modules: set[RelativeDirPath | str] | None = Field(
        description="Set of modules to build. Either module names (folder names) or relative paths"
        " to the organization directory. This can be empty"
    )
    variables: dict[str, JsonValue] = Field(description="Variables to be used during the build process.")
    path: AbsoluteFilePath = Field(
        description="Path to the configuration YAML file (typically config.<env>.yaml under the organization directory that contains the variables)."
    )


class BuildInput(BaseModel):
    """The input files after reading the source system.

    All yaml files found in the modules/ directory.
    If available, the config.<name>.yaml file, which specifies which modules to build, variables available,
    CDF Project to build for.
    """

    yaml_files: list[RelativeFilePath] = Field(
        description="List of all YAML files that are part of the build, with paths relative to the organization directory."
    )
    config_files: list[ConfigFile] | None = Field(
        None,
        description="List of configuration YAML files to be used during the build process. Each file corresponds to a set of variables and "
        "selected modules.",
    )
    validation_type: ValidationType = "prod"
    cdf_project: str
    organization_dir: AbsoluteDirPath
    deploy_branch: str | None = None
    deploy_protection: DeployProtection | None = None

    user_selected_modules: set[RelativeDirPath | str] | None = Field(
        description="Set of modules to build. Either module names (folder names) or relative paths to the organization directory."
        "If None, no modules are selected."
    )

    @property
    def module_dir(self) -> Path:
        return self.organization_dir / MODULES


@dataclass
class ValidatableResource(Generic[T_RequestResource]):
    identifier: Identifier
    type: ResourceType
    source: Path | Literal["cdf"]
    request: T_RequestResource


class BuiltResource(BaseModel):
    """Represents a resource that has been built from the source system."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    identifier: Identifier
    source_hash: str
    type: ResourceType
    source_type: ResourceType | None = Field(
        default=None,
        description="File format of the source resource. When the source is a ResourceFileIO this differs from type, "
        "which is the CRUD type written to CDF. For example, a CatalogDataSet is deployed as a DataSet.",
    )
    source_path: AbsoluteFilePath
    build_path: AbsoluteFilePath
    crud_cls: builtins.type[ResourceIO]
    dependencies: set[tuple[builtins.type[ResourceIO], Identifier]] = Field(default_factory=set)

    failed_extra: list[FailedReadExtra] = Field(default_factory=list)
    extra_files: list[SuccessExtra] = Field(default_factory=list)

    has_syntax_error: bool
    variables: list[BuildVariable] = Field(default_factory=list)
    module_id: ModuleId

    @property
    def source(self) -> Path | Literal["cdf"]:
        """The source of the resource, either a file path or 'cdf' if the resource was retrieved from CDF."""
        return self.source_path

    @property
    def has_valid_syntax(self) -> bool:
        """Indicates whether the resource has valid syntax and can be further validated."""
        return not self.has_syntax_error and not self.failed_extra

    def as_request(
        self, crud: ResourceIO[T_Identifier, T_RequestResource, Any, Any], is_dry_run: bool = False
    ) -> ValidatableResource[T_RequestResource]:
        """Convert the built resource to a ValidatableResource for the given CRUD.

        Args:
            crud: The CRUD class to use for converting the built resource to a request object.
            is_dry_run: Whether to load the resource in dry-run mode, i.e., without requiring lookups in CDF to succeed.
        Returns:
            A ValidatableResource containing the identifier, type, source, and request object for the built
            resource.
        """
        if not isinstance(crud, self.crud_cls):
            raise RuntimeError(
                f"Bug in Toolkit, cannot convert built resource of type {self.crud_cls} to request for CRUD {crud}. "
                f"Expected a subclass of {self.crud_cls}."
            )
        raw_list = crud.load_resource_file(self.build_path)
        if not raw_list:
            raise RuntimeError(
                f"Bug in Toolkit, built resource {self.build_path} has no data to convert to request for CRUD {crud}."
            )
        request = crud.load_resource(raw_list[0], is_dry_run=is_dry_run)
        if isinstance(request, FunctionRequest):
            request.function_code = FunctionIO.get_function_code_implicitly(self.source_path, request.as_id())
        return ValidatableResource(
            identifier=self.identifier,
            type=self.type,
            source=self.source,
            request=request,
        )


class BuiltModule(BaseModel):
    module_id: ModuleId
    resources: list[BuiltResource] = Field(default_factory=list)
    insights: list[Insight] = Field(default_factory=list)
    syntax_errors_by_source: dict[Path, list[BuildError]] = Field(default_factory=dict)
    syntax_warnings_by_source: dict[Path, list[BuildWarning | Recommendation]] = Field(default_factory=dict)
    unresolved_variables_by_source: dict[Path, list[str]] = Field(default_factory=dict)
    failed_files: list[FailedReadYAMLFile] = Field(default_factory=list)
    ignored_files: list[IgnoredFile] = Field(default_factory=list)
    ignore_rules_by_source: dict[Path, set[str]] = Field(default_factory=dict)
    yaml_line_count: int
    variables: list[BuildVariable] = Field(default_factory=list)

    @property
    def files_built(self) -> bool:
        """Indicates whether any files were built for this module."""
        return len(self.resources) > 0

    @property
    def is_success(self) -> bool:
        """Determines if the module build was successful based on the presence of built file and validation errors."""
        return self.files_built

    @property
    def all_insights(self) -> InsightList:
        """The list of all insights for this module."""
        insights = InsightList(self.insights)
        for resource in self.resources:
            for failed_extra in resource.failed_extra:
                insights.append(
                    BuildError(
                        message=failed_extra.error,
                        code=failed_extra.code,
                        source=resource.source_path,
                    )
                )
        for path, errors in self.syntax_errors_by_source.items():
            insights.extend(errors)
        for path, warnings in self.syntax_warnings_by_source.items():
            insights.extend(warnings)
        for path, variables in self.unresolved_variables_by_source.items():
            insights.extend(self._unresolved_variable_insights(path, variables))
        for failed_file in self.failed_files:
            if failed_file.code == "INVALID-FILE-CONTENT" and failed_file.unresolved_variables:
                # An unresolved placeholder such as `key: {{ variable }}` is not valid YAML. The unresolved
                # variables are the root cause and are already reported as their own insight.
                continue
            insights.append(
                BuildError(
                    code=failed_file.code,
                    message=failed_file.error,
                    source=failed_file.source_path,
                )
            )
        for ignored_file in self.ignored_files:
            insights.append(
                BuildWarning(
                    code=ignored_file.code,
                    message=ignored_file.reason,
                    fix=ignored_file.fix,
                    source=ignored_file.filepath,
                )
            )

        return insights

    @classmethod
    def _unresolved_variable_insights(cls, path: Path, variables: list[str]) -> list[Insight]:
        """One insight per variable, such that all files missing the same variable are grouped when displayed."""
        insights: list[Insight] = []
        for variable in variables:
            position = find_unique_variable_position(path, variable)
            insights.append(
                BuildError(
                    code="UNRESOLVED-VARIABLE",
                    message=f"Unresolved variable {{{{ {variable} }}}}",
                    fix="Make sure to define the variable in the 'config.<env>.yaml' file and that it is "
                    "correctly placed in the variables section matching the file path",
                    source=path,
                    line=position.line if position else None,
                    column=position.column if position else None,
                )
            )
        return insights

    def __hash__(self) -> int:
        return hash(self.module_id.path)


class ValidationResult(BaseModel):
    name: str
    insights: list[Insight]
    errors: list[InternalValidatorException]


class ValidationResultList(UserList[ValidationResult]):
    @property
    def insights(self) -> InsightList:
        """Aggregates all insights from the validation results."""
        insights = InsightList()
        for result in self.data:
            insights.extend(result.insights)
        return insights


class BuildFolder(BaseModel):
    """Built folder acts as a container holding all built modules and insights from the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    organization_dir: AbsoluteDirPath
    build_dir: AbsoluteDirPath
    started_at: datetime
    finished_at: datetime
    deploy_branch: str | None = Field(
        default=None,
        description="Git branch that is expected to deploy the build.",
    )
    deploy_protection: DeployProtection | None = Field(
        default=None,
        description="Whether the build is protected from deployment on non-deploy branches.",
    )
    config_hash: str | None = None

    built_modules: list[BuiltModule] = Field(default_factory=list)
    all_variables: list[BuildVariable] = Field(default_factory=list)

    @property
    def module_insights(self) -> InsightList:
        """Insights from all built modules."""
        insights = InsightList()
        for module in self.built_modules:
            insights.extend(module.all_insights)
        return insights

    @property
    def build_duration_seconds(self) -> float:
        """Duration of the build in seconds."""
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def rules_ignored_by_source(self) -> dict[Path, set[str]]:
        """Aggregates all ignored rules from all built modules, grouped by source file path."""
        ignored_rules: dict[Path, set[str]] = {}
        for module in self.built_modules:
            for source_path, rules in module.ignore_rules_by_source.items():
                if source_path not in ignored_rules:
                    ignored_rules[source_path] = set()
                ignored_rules[source_path].update(rules)
        return ignored_rules
