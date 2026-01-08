"""
Example implementations for the Build Command v2 plan.

These examples show how the proposed architecture would work in practice.
They are not meant to be used directly but to illustrate the design.
"""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

# =============================================================================
# PART 1: Issue System Examples
# =============================================================================


class IssueSeverity(Enum):
    """Severity levels for build issues."""

    ERROR = "error"
    WARNING = "warning"
    HINT = "hint"


class IssueCategory(Enum):
    """Categories for grouping issues."""

    MODULE = "MOD"
    VARIABLE = "VAR"
    RESOURCE = "RES"
    DEPENDENCY = "DEP"
    CONFIG = "CFG"


@dataclass(frozen=True, kw_only=True)
class BuildIssue:
    """Base class for all build issues."""

    code: ClassVar[str]
    severity: ClassVar[IssueSeverity]
    category: ClassVar[IssueCategory]

    path: Path | None = None

    @property
    def message(self) -> str:
        raise NotImplementedError

    @property
    def fix(self) -> str | None:
        return None

    def format(self, verbose: bool = False) -> str:
        parts = [f"[{self.code}]"]
        if self.path:
            parts.append(f"({self.path})")
        parts.append(self.message)
        if verbose and self.fix:
            parts.append(f"\n  → Fix: {self.fix}")
        return " ".join(parts)


# =============================================================================
# Module Issues (MOD_xxx)
# =============================================================================


@dataclass(frozen=True, kw_only=True)
class ModuleNotFoundIssue(BuildIssue):
    """Module directory was not found.

    ## What happened
    The specified module path does not exist or is not accessible.

    ## How to fix
    Check that the module path is correct and the directory exists.
    """

    code: ClassVar[str] = "MOD_001"
    severity: ClassVar[IssueSeverity] = IssueSeverity.ERROR
    category: ClassVar[IssueCategory] = IssueCategory.MODULE

    searched_locations: list[Path]

    @property
    def message(self) -> str:
        return "Module directory not found"

    @property
    def fix(self) -> str:
        locations = ", ".join(str(p) for p in self.searched_locations)
        return f"Create a 'modules' directory or check the path. Searched: {locations}"


@dataclass(frozen=True, kw_only=True)
class ModuleStructureIssue(BuildIssue):
    """Module has invalid structure.

    ## What happened
    The module directory exists but has an invalid structure.
    A valid module must contain at least one resource folder.

    ## How to fix
    Add resource folders (e.g., data_modeling, transformations) to the module.
    """

    code: ClassVar[str] = "MOD_002"
    severity: ClassVar[IssueSeverity] = IssueSeverity.ERROR
    category: ClassVar[IssueCategory] = IssueCategory.MODULE

    reason: str

    @property
    def message(self) -> str:
        return f"Invalid module structure: {self.reason}"


@dataclass(frozen=True, kw_only=True)
class UnrecognizedResourceFolderIssue(BuildIssue):
    """Module contains an unrecognized resource folder.

    ## What happened
    A folder inside the module is not a known resource type and will be skipped.

    ## How to fix
    Either rename the folder to a valid resource type or remove it.
    """

    code: ClassVar[str] = "MOD_003"
    severity: ClassVar[IssueSeverity] = IssueSeverity.WARNING
    category: ClassVar[IssueCategory] = IssueCategory.MODULE

    folder_name: str
    known_folders: list[str]

    @property
    def message(self) -> str:
        return f"Unrecognized resource folder '{self.folder_name}' will be skipped"

    @property
    def fix(self) -> str:
        examples = ", ".join(self.known_folders[:5])
        return f"Rename to a valid resource type (e.g., {examples})"


@dataclass(frozen=True, kw_only=True)
class DisabledResourceFolderIssue(BuildIssue):
    """Module contains a disabled resource folder.

    ## What happened
    A resource type is present but requires explicit enabling in cdf.toml.

    ## How to fix
    Add the required feature flag to cdf.toml.
    """

    code: ClassVar[str] = "MOD_004"
    severity: ClassVar[IssueSeverity] = IssueSeverity.WARNING
    category: ClassVar[IssueCategory] = IssueCategory.MODULE

    folder_name: str

    @property
    def message(self) -> str:
        return f"Resource folder '{self.folder_name}' is disabled"

    @property
    def fix(self) -> str:
        return f"Enable '{self.folder_name}' in cdf.toml under [features]"


# =============================================================================
# Variable Issues (VAR_xxx)
# =============================================================================


@dataclass(frozen=True, kw_only=True)
class UnresolvedVariableIssue(BuildIssue):
    """A template variable was not resolved.

    ## What happened
    A {{ variable }} placeholder in the resource file has no corresponding
    value in the configuration.

    ## How to fix
    Add the variable to config.[env].yaml in the appropriate section.
    """

    code: ClassVar[str] = "VAR_001"
    severity: ClassVar[IssueSeverity] = IssueSeverity.WARNING
    category: ClassVar[IssueCategory] = IssueCategory.VARIABLE

    variable_name: str
    original_text: str
    suggested_location: str | None = None

    @property
    def message(self) -> str:
        return f"Unresolved variable '{self.original_text}'"

    @property
    def fix(self) -> str:
        if self.suggested_location:
            return f"Add '{self.variable_name}' to {self.suggested_location}"
        return f"Add '{self.variable_name}' to your config.[env].yaml"


@dataclass(frozen=True, kw_only=True)
class InvalidVariableValueIssue(BuildIssue):
    """Variable value has invalid type or format.

    ## What happened
    The value for a variable doesn't match the expected type.

    ## How to fix
    Update the variable value to the correct type.
    """

    code: ClassVar[str] = "VAR_002"
    severity: ClassVar[IssueSeverity] = IssueSeverity.ERROR
    category: ClassVar[IssueCategory] = IssueCategory.VARIABLE

    variable_name: str
    expected_type: str
    actual_value: Any

    @property
    def message(self) -> str:
        actual_type = type(self.actual_value).__name__
        return f"Variable '{self.variable_name}' has invalid type. Expected {self.expected_type}, got {actual_type}"


@dataclass(frozen=True, kw_only=True)
class TemplateVariableNotReplacedIssue(BuildIssue):
    """A template placeholder like <change_me> was not replaced.

    ## What happened
    The configuration contains a placeholder value that should be replaced
    with an actual value before building.

    ## How to fix
    Replace the placeholder with the appropriate value.
    """

    code: ClassVar[str] = "VAR_005"
    severity: ClassVar[IssueSeverity] = IssueSeverity.WARNING
    category: ClassVar[IssueCategory] = IssueCategory.VARIABLE

    variable_name: str
    placeholder_value: str

    @property
    def message(self) -> str:
        return f"Template placeholder '{self.placeholder_value}' not replaced in '{self.variable_name}'"

    @property
    def fix(self) -> str:
        return f"Replace {self.placeholder_value} with the actual value"


# =============================================================================
# Resource Issues (RES_xxx)
# =============================================================================


@dataclass(frozen=True, kw_only=True)
class ResourceValidationIssue(BuildIssue):
    """Resource failed validation.

    ## What happened
    The resource YAML doesn't conform to the expected schema.

    ## How to fix
    Update the resource to match the required format.
    """

    code: ClassVar[str] = "RES_002"
    severity: ClassVar[IssueSeverity] = IssueSeverity.ERROR
    category: ClassVar[IssueCategory] = IssueCategory.RESOURCE

    resource_type: str
    validation_errors: list[str]

    @property
    def message(self) -> str:
        errors = "; ".join(self.validation_errors[:3])
        if len(self.validation_errors) > 3:
            errors += f" (+{len(self.validation_errors) - 3} more)"
        return f"{self.resource_type} validation failed: {errors}"


@dataclass(frozen=True, kw_only=True)
class DuplicateResourceIssue(BuildIssue):
    """Duplicate resource identifier found.

    ## What happened
    Two or more resources have the same identifier.

    ## How to fix
    Ensure all resources have unique identifiers.
    """

    code: ClassVar[str] = "RES_004"
    severity: ClassVar[IssueSeverity] = IssueSeverity.WARNING
    category: ClassVar[IssueCategory] = IssueCategory.RESOURCE

    identifier: str
    first_location: Path

    @property
    def message(self) -> str:
        return f"Duplicate resource '{self.identifier}' (first seen in {self.first_location})"

    @property
    def fix(self) -> str:
        return "Use unique identifiers for each resource"


# =============================================================================
# Issue Collection
# =============================================================================


class IssueCollector:
    """Collects and organizes build issues.

    Example usage:
        collector = IssueCollector()
        collector.add(ModuleNotFoundIssue(path=Path("modules")))

        if collector.has_errors:
            for error in collector.errors:
                print(error.format(verbose=True))
    """

    def __init__(self) -> None:
        self._issues: list[BuildIssue] = []

    def add(self, issue: BuildIssue) -> None:
        """Add an issue to the collection."""
        self._issues.append(issue)

    def extend(self, other: IssueCollector) -> None:
        """Add all issues from another collector."""
        self._issues.extend(other._issues)

    @property
    def has_errors(self) -> bool:
        """Check if there are any fatal errors."""
        return any(i.severity == IssueSeverity.ERROR for i in self._issues)

    @property
    def errors(self) -> list[BuildIssue]:
        return [i for i in self._issues if i.severity == IssueSeverity.ERROR]

    @property
    def warnings(self) -> list[BuildIssue]:
        return [i for i in self._issues if i.severity == IssueSeverity.WARNING]

    @property
    def hints(self) -> list[BuildIssue]:
        return [i for i in self._issues if i.severity == IssueSeverity.HINT]

    def by_category(self) -> dict[IssueCategory, list[BuildIssue]]:
        """Group issues by category."""
        grouped: dict[IssueCategory, list[BuildIssue]] = defaultdict(list)
        for issue in self._issues:
            grouped[issue.category].append(issue)
        return dict(grouped)

    def print_summary(self) -> None:
        """Print a formatted summary of all issues."""
        if not self._issues:
            print("✓ No issues found")
            return

        print(f"\n{'=' * 60}")
        print(f"BUILD ISSUES ({len(self._issues)} total)")
        print(f"{'=' * 60}\n")

        # Group by severity
        for severity, color in [
            (IssueSeverity.ERROR, "🔴"),
            (IssueSeverity.WARNING, "🟡"),
            (IssueSeverity.HINT, "🔵"),
        ]:
            issues = [i for i in self._issues if i.severity == severity]
            if not issues:
                continue

            print(f"{color} {severity.value.upper()}S ({len(issues)}):")
            for issue in issues:
                print(f"   {issue.format()}")
            print()


# =============================================================================
# PART 2: Module Loading Example
# =============================================================================


class ResourceFolder(BaseModel):
    """A resource folder within a module."""

    model_config = ConfigDict(frozen=True)

    name: str
    path: Path
    files: list[Path] = Field(default_factory=list)


class Module(BaseModel):
    """A module directory containing resources."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    path: Path
    resource_folders: dict[str, ResourceFolder] = Field(default_factory=dict)

    @property
    def resource_folder_names(self) -> set[str]:
        return set(self.resource_folders.keys())


class ModuleCollection(BaseModel):
    """Collection of loaded modules."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    organization_dir: Path
    modules: list[Module] = Field(default_factory=list)

    def __len__(self) -> int:
        return len(self.modules)

    def __iter__(self):
        return iter(self.modules)


# =============================================================================
# PART 3: Module Loader Example
# =============================================================================


class ModuleLoader:
    """Loads modules from the filesystem with comprehensive error handling.

    Example usage:
        loader = ModuleLoader(Path("/project"))
        modules = loader.load(selection=["my_module"])

        if loader.issues.has_errors:
            loader.issues.print_summary()
            return

        for module in modules:
            print(f"Loaded: {module.name}")
    """

    # Known resource folder names
    KNOWN_FOLDERS: ClassVar[set[str]] = {
        "auth",
        "data_modeling",
        "data_sets",
        "extraction_pipelines",
        "files",
        "functions",
        "groups",
        "labels",
        "locations",
        "raw",
        "transformations",
        "workflows",
    }

    # Folders that require enabling
    DISABLED_FOLDERS: ClassVar[set[str]] = {
        "classic",
    }

    def __init__(self, organization_dir: Path) -> None:
        self.organization_dir = organization_dir
        self.modules_root = organization_dir / "modules"
        self.issues = IssueCollector()

    def load(self, selection: list[str | Path] | None = None) -> ModuleCollection:
        """Load modules from the filesystem."""
        # Check modules root exists
        if not self.modules_root.exists():
            self.issues.add(
                ModuleNotFoundIssue(
                    path=self.modules_root,
                    searched_locations=[self.modules_root],
                )
            )
            return ModuleCollection(
                organization_dir=self.organization_dir,
                modules=[],
            )

        # Discover modules
        module_paths = self._discover_modules()

        # Filter by selection
        if selection:
            module_paths = self._filter_by_selection(module_paths, selection)

        # Load each module
        modules = []
        for path in module_paths:
            module = self._load_module(path)
            if module:
                modules.append(module)

        return ModuleCollection(
            organization_dir=self.organization_dir,
            modules=modules,
        )

    def _discover_modules(self) -> list[Path]:
        """Find all module directories."""
        modules: list[Path] = []

        for dirpath, dirnames, _ in os.walk(self.modules_root):
            current = Path(dirpath)

            # Check if this directory contains resource folders
            has_resources = any(name in self.KNOWN_FOLDERS or name in self.DISABLED_FOLDERS for name in dirnames)

            if has_resources:
                modules.append(current)
                dirnames.clear()  # Don't descend further

        return modules

    def _load_module(self, path: Path) -> Module | None:
        """Load a single module."""
        resource_folders: dict[str, ResourceFolder] = {}

        for item in path.iterdir():
            if not item.is_dir():
                continue

            name = item.name

            # Check folder type
            if name in self.DISABLED_FOLDERS:
                self.issues.add(
                    DisabledResourceFolderIssue(
                        path=path,
                        folder_name=name,
                    )
                )
                continue

            if name not in self.KNOWN_FOLDERS:
                self.issues.add(
                    UnrecognizedResourceFolderIssue(
                        path=path,
                        folder_name=name,
                        known_folders=sorted(self.KNOWN_FOLDERS),
                    )
                )
                continue

            # Load files
            files = list(item.rglob("*.yaml")) + list(item.rglob("*.yml"))

            resource_folders[name] = ResourceFolder(
                name=name,
                path=item,
                files=files,
            )

        if not resource_folders:
            self.issues.add(
                ModuleStructureIssue(
                    path=path,
                    reason="No valid resource folders",
                )
            )
            return None

        return Module(
            name=path.name,
            path=path,
            resource_folders=resource_folders,
        )

    def _filter_by_selection(self, paths: list[Path], selection: list[str | Path]) -> list[Path]:
        """Filter modules by user selection."""
        selected = []
        selection_set = {str(s).lower() for s in selection}

        for path in paths:
            # Match by name or path
            if path.name.lower() in selection_set:
                selected.append(path)
            elif any(str(path).lower().endswith(str(s).lower()) for s in selection):
                selected.append(path)

        return selected


# =============================================================================
# PART 4: Usage Example
# =============================================================================


def example_build() -> None:
    """Example of how the new build system would work."""
    # Setup
    project_dir = Path("/my/project")

    # Load modules
    loader = ModuleLoader(project_dir)
    modules = loader.load(selection=["core_module", "extension_module"])

    # Check for loading issues
    if loader.issues.has_errors:
        print("❌ Failed to load modules:")
        loader.issues.print_summary()
        return

    print(f"✓ Loaded {len(modules)} modules")

    # Collect all issues
    all_issues = IssueCollector()
    all_issues.extend(loader.issues)

    # Process each module
    for module in modules:
        print(f"  Processing: {module.name}")

        for folder_name, folder in module.resource_folders.items():
            print(f"    - {folder_name}: {len(folder.files)} files")

    # Print final summary
    all_issues.print_summary()


if __name__ == "__main__":
    example_build()
