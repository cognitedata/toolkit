# Build Command v2 - Architecture Plan

## Overview

This document outlines the architecture for the new build command. The primary goals are:

1. **Load modules as Pydantic data classes** - Type-safe, validated configuration
2. **Perform variable replacement** - Template substitution with clear error reporting
3. **Populate build folder** - Create deployable artifacts
4. **Collect issues throughout** - Warnings, errors, and recommendations with codes
5. **Pluggable validation system** - Extensible for future checks

## Architecture Principles

Following the Gemini style guide for readable, maintainable code:

- **Small, focused functions** - Each function does one thing well
- **Clear naming** - Names describe purpose, not implementation
- **Explicit over implicit** - No magic, clear data flow
- **Type hints everywhere** - Pydantic models for structured data
- **Fail fast, fail clearly** - Early validation with actionable error messages

---

## 1. Issue & Warning System (Foundation)

### Issue Hierarchy

```text
BuildIssue (base)
├── BuildError      - Fatal, stops build
├── BuildWarning    - Non-fatal, build continues
└── BuildHint       - Recommendation for improvement
```

### Issue Design

Each issue has:

- **Code**: Unique identifier (e.g., `MOD_001`, `VAR_002`, `RES_003`)
- **Severity**: `error`, `warning`, `hint`
- **Category**: `module`, `variable`, `resource`, `structure`, `dependency`
- **Message**: Human-readable description
- **Location**: Where the issue occurred (file, line, module)
- **Fix suggestion**: How to resolve (optional)

### File: `cognite_toolkit/_cdf_tk/commands/build_v2/issues/__init__.py`

```python
"""
Build issue system for collecting and reporting problems during the build process.

Issue codes follow the pattern: {CATEGORY}_{NUMBER}

Categories:
- MOD: Module loading issues
- VAR: Variable replacement issues  
- RES: Resource validation issues
- DEP: Dependency issues
- CFG: Configuration issues
"""

from ._base import BuildIssue, BuildError, BuildWarning, BuildHint, IssueSeverity
from ._module_issues import (
    ModuleNotFoundIssue,
    ModuleStructureIssue,
    UnrecognizedResourceFolderIssue,
    DisabledResourceFolderIssue,
    DuplicateModuleIssue,
)
from ._variable_issues import (
    UnresolvedVariableIssue,
    InvalidVariableValueIssue,
    MissingRequiredVariableIssue,
)
from ._resource_issues import (
    ResourceValidationIssue,
    DuplicateResourceIssue,
    MissingDependencyIssue,
)
from ._collection import IssueCollector

__all__ = [
    "BuildIssue",
    "BuildError", 
    "BuildWarning",
    "BuildHint",
    "IssueSeverity",
    "IssueCollector",
    # Module issues
    "ModuleNotFoundIssue",
    "ModuleStructureIssue",
    "UnrecognizedResourceFolderIssue",
    "DisabledResourceFolderIssue",
    "DuplicateModuleIssue",
    # Variable issues
    "UnresolvedVariableIssue",
    "InvalidVariableValueIssue",
    "MissingRequiredVariableIssue",
    # Resource issues
    "ResourceValidationIssue",
    "DuplicateResourceIssue",
    "MissingDependencyIssue",
]
```

### File: `issues/_base.py`

```python
"""Base classes for build issues."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import ClassVar


class IssueSeverity(Enum):
    """Severity levels for build issues."""
    ERROR = "error"      # Fatal - build cannot continue
    WARNING = "warning"  # Non-fatal - build continues but may have problems
    HINT = "hint"        # Recommendation - everything works but could be better


class IssueCategory(Enum):
    """Categories of build issues for grouping and filtering."""
    MODULE = "MOD"      # Module loading and structure
    VARIABLE = "VAR"    # Variable replacement
    RESOURCE = "RES"    # Resource validation
    DEPENDENCY = "DEP"  # Dependency resolution
    CONFIG = "CFG"      # Configuration issues


@dataclass(frozen=True, kw_only=True)
class BuildIssue(ABC):
    """
    Base class for all build issues.
    
    Issues are immutable and uniquely identified by their code.
    They carry enough context to display helpful messages to users.
    """
    
    # Class variables - set in subclasses
    code: ClassVar[str]
    severity: ClassVar[IssueSeverity]
    category: ClassVar[IssueCategory]
    
    # Instance variables - vary per occurrence
    path: Path | None = None
    line: int | None = None
    
    @property
    @abstractmethod
    def message(self) -> str:
        """Human-readable description of the issue."""
        ...
    
    @property
    def fix(self) -> str | None:
        """Optional suggestion for how to fix the issue."""
        return None
    
    @property
    def location(self) -> str:
        """Format the location for display."""
        if self.path is None:
            return ""
        if self.line is not None:
            return f"{self.path}:{self.line}"
        return str(self.path)
    
    def format(self, verbose: bool = False) -> str:
        """Format the issue for display."""
        parts = [f"[{self.code}]", self.message]
        if self.location:
            parts.insert(1, f"({self.location})")
        if verbose and self.fix:
            parts.append(f"\n  Fix: {self.fix}")
        return " ".join(parts)


class BuildError(BuildIssue):
    """Fatal issue that prevents the build from completing."""
    severity: ClassVar[IssueSeverity] = IssueSeverity.ERROR


class BuildWarning(BuildIssue):
    """Non-fatal issue - build continues but may have problems."""
    severity: ClassVar[IssueSeverity] = IssueSeverity.WARNING


class BuildHint(BuildIssue):
    """Recommendation for improvement - everything works."""
    severity: ClassVar[IssueSeverity] = IssueSeverity.HINT
```

### File: `issues/_collection.py`

```python
"""Issue collection and reporting."""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator

from rich.console import Console
from rich.table import Table

from ._base import BuildIssue, IssueSeverity, IssueCategory


@dataclass
class IssueCollector:
    """
    Collects issues during the build process.
    
    Thread-safe collection that groups issues by category and severity.
    Provides methods for filtering and formatting issues for display.
    """
    
    _issues: list[BuildIssue] = field(default_factory=list)
    
    def add(self, issue: BuildIssue) -> None:
        """Add an issue to the collection."""
        self._issues.append(issue)
    
    def extend(self, issues: "IssueCollector | list[BuildIssue]") -> None:
        """Add multiple issues."""
        if isinstance(issues, IssueCollector):
            self._issues.extend(issues._issues)
        else:
            self._issues.extend(issues)
    
    def __iter__(self) -> Iterator[BuildIssue]:
        return iter(self._issues)
    
    def __len__(self) -> int:
        return len(self._issues)
    
    def __bool__(self) -> bool:
        return bool(self._issues)
    
    @property
    def has_errors(self) -> bool:
        """Check if there are any fatal errors."""
        return any(i.severity == IssueSeverity.ERROR for i in self._issues)
    
    @property
    def errors(self) -> list[BuildIssue]:
        """Get all errors."""
        return [i for i in self._issues if i.severity == IssueSeverity.ERROR]
    
    @property
    def warnings(self) -> list[BuildIssue]:
        """Get all warnings."""
        return [i for i in self._issues if i.severity == IssueSeverity.WARNING]
    
    @property
    def hints(self) -> list[BuildIssue]:
        """Get all hints."""
        return [i for i in self._issues if i.severity == IssueSeverity.HINT]
    
    def by_category(self) -> dict[IssueCategory, list[BuildIssue]]:
        """Group issues by category."""
        grouped: dict[IssueCategory, list[BuildIssue]] = defaultdict(list)
        for issue in self._issues:
            grouped[issue.category].append(issue)
        return dict(grouped)
    
    def print_summary(self, console: Console | None = None) -> None:
        """Print a summary of all issues."""
        if not self._issues:
            return
        
        console = console or Console()
        
        # Group by severity
        for severity in IssueSeverity:
            issues = [i for i in self._issues if i.severity == severity]
            if not issues:
                continue
            
            color = {"error": "red", "warning": "yellow", "hint": "blue"}[severity.value]
            console.print(f"\n[bold {color}]{severity.value.upper()}S ({len(issues)}):[/]")
            
            for issue in issues:
                console.print(f"  {issue.format()}")
```

---

## 2. Module Loading System

### Design Goals

- **Clear error messages** - User knows exactly what went wrong and how to fix it
- **Incremental loading** - Load as much as possible, collect all errors
- **Pydantic validation** - Type-safe, validated module structure
- **Lazy loading** - Don't read file contents until needed

### File: `data_classes/_module.py`

```python
"""
Pydantic models for module representation.

A module is a directory containing resource folders (e.g., data_modeling, transformations).
Modules can have a definition file (module.toml) with metadata.
"""

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml


class ResourceFolder(BaseModel):
    """Represents a resource folder within a module."""
    
    model_config = ConfigDict(frozen=True)
    
    name: str
    path: Path
    files: list[Path] = Field(default_factory=list)
    
    @property
    def file_count(self) -> int:
        return len(self.files)


class Module(BaseModel):
    """
    Represents a single module directory.
    
    A module is the unit of organization for resources. It contains:
    - Resource folders (data_modeling, transformations, etc.)
    - Optional module.toml with metadata
    - Variables that can be substituted during build
    """
    
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    
    # Identity
    name: str
    path: Path
    
    # Content
    resource_folders: dict[str, ResourceFolder] = Field(default_factory=dict)
    definition: ModuleToml | None = None
    
    # Metadata (from definition or inferred)
    package_id: str | None = None
    module_id: str | None = None
    
    @property
    def relative_path(self) -> Path:
        """Path relative to the modules root."""
        # Find 'modules' in parents and return relative
        for i, part in enumerate(self.path.parts):
            if part.lower() == "modules":
                return Path(*self.path.parts[i+1:])
        return self.path
    
    @property
    def resource_folder_names(self) -> set[str]:
        return set(self.resource_folders.keys())


class ModuleCollection(BaseModel):
    """
    Collection of modules loaded from an organization directory.
    
    Provides methods for filtering and accessing modules.
    """
    
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    
    organization_dir: Path
    modules: list[Module] = Field(default_factory=list)
    
    def __iter__(self):
        return iter(self.modules)
    
    def __len__(self) -> int:
        return len(self.modules)
    
    def get_by_name(self, name: str) -> Module | None:
        """Get a module by name (case-insensitive)."""
        name_lower = name.lower()
        for module in self.modules:
            if module.name.lower() == name_lower:
                return module
        return None
    
    def filter_by_selection(self, selection: list[str | Path]) -> "ModuleCollection":
        """Return a new collection with only selected modules."""
        # Implementation details...
        pass
    
    @property
    def all_resource_folders(self) -> set[str]:
        """Get all unique resource folder names across all modules."""
        folders: set[str] = set()
        for module in self.modules:
            folders.update(module.resource_folder_names)
        return folders
```

### File: `loaders/_module_loader.py`

```python
"""
Module loading with comprehensive error handling.

This is where we convert filesystem structure to Pydantic models,
collecting issues along the way.
"""

import os
from pathlib import Path

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, EXCLUDED_CRUDS

from ..data_classes import Module, ModuleCollection, ResourceFolder
from ..issues import (
    IssueCollector,
    ModuleNotFoundIssue,
    ModuleStructureIssue,
    UnrecognizedResourceFolderIssue,
    DisabledResourceFolderIssue,
)


class ModuleLoader:
    """
    Loads modules from the filesystem.
    
    This class is responsible for:
    - Discovering module directories
    - Validating module structure
    - Creating Pydantic models
    - Collecting issues encountered during loading
    """
    
    def __init__(self, organization_dir: Path):
        self.organization_dir = organization_dir
        self.modules_root = organization_dir / MODULES
        self.issues = IssueCollector()
        
        # Known resource folder names
        self._known_folders = set(CRUDS_BY_FOLDER_NAME.keys())
        self._disabled_folders = {crud.folder_name for crud in EXCLUDED_CRUDS}
    
    def load(self, selection: list[str | Path] | None = None) -> ModuleCollection:
        """
        Load modules from the filesystem.
        
        Args:
            selection: Optional list of module names or paths to load.
                      If None, loads all modules.
        
        Returns:
            ModuleCollection with all loaded modules.
            Issues are accumulated in self.issues.
        """
        if not self.modules_root.exists():
            self.issues.add(ModuleNotFoundIssue(
                path=self.modules_root,
                searched_locations=[self.modules_root],
            ))
            return ModuleCollection(
                organization_dir=self.organization_dir,
                modules=[],
            )
        
        # Discover module candidates
        module_paths = self._discover_module_paths()
        
        # Filter by selection if provided
        if selection:
            module_paths = self._filter_by_selection(module_paths, selection)
        
        # Load each module
        modules = []
        for path in module_paths:
            module = self._load_single_module(path)
            if module:
                modules.append(module)
        
        return ModuleCollection(
            organization_dir=self.organization_dir,
            modules=modules,
        )
    
    def _discover_module_paths(self) -> list[Path]:
        """
        Find all module directories.
        
        A directory is a module if it directly contains resource folders.
        Resource folders are leaf directories with files.
        """
        modules: list[Path] = []
        
        for dirpath, dirnames, filenames in os.walk(self.modules_root):
            current = Path(dirpath)
            
            # Check if any subdirectory is a known resource folder
            has_resource_folder = any(
                name in self._known_folders or name in self._disabled_folders
                for name in dirnames
            )
            
            if has_resource_folder:
                modules.append(current)
                # Don't descend into modules (remove subdirs from walk)
                dirnames.clear()
        
        return modules
    
    def _load_single_module(self, path: Path) -> Module | None:
        """Load a single module from a directory."""
        resource_folders: dict[str, ResourceFolder] = {}
        
        for item in path.iterdir():
            if not item.is_dir():
                continue
            
            folder_name = item.name
            
            # Check if it's a known resource folder
            if folder_name in self._disabled_folders:
                self.issues.add(DisabledResourceFolderIssue(
                    path=path,
                    folder_name=folder_name,
                ))
                continue
            
            if folder_name not in self._known_folders:
                self.issues.add(UnrecognizedResourceFolderIssue(
                    path=path,
                    folder_name=folder_name,
                    known_folders=sorted(self._known_folders),
                ))
                continue
            
            # Load files in resource folder
            files = list(item.rglob("*.yaml")) + list(item.rglob("*.yml"))
            
            resource_folders[folder_name] = ResourceFolder(
                name=folder_name,
                path=item,
                files=files,
            )
        
        if not resource_folders:
            self.issues.add(ModuleStructureIssue(
                path=path,
                reason="No valid resource folders found",
            ))
            return None
        
        # Load module definition if present
        definition = self._load_module_definition(path)
        
        return Module(
            name=path.name,
            path=path,
            resource_folders=resource_folders,
            definition=definition,
            package_id=definition.package_id if definition else None,
            module_id=definition.module_id if definition else None,
        )
    
    def _load_module_definition(self, path: Path) -> ModuleToml | None:
        """Load module.toml if it exists."""
        toml_path = path / "module.toml"
        if toml_path.exists():
            return ModuleToml.load(toml_path)
        return None
    
    def _filter_by_selection(
        self, 
        paths: list[Path], 
        selection: list[str | Path]
    ) -> list[Path]:
        """Filter module paths by user selection."""
        # Implementation details...
        pass
```

---

## 3. Variable Replacement System

### Design Goals

- **Identify all variables before replacement** - Know what's needed
- **Track variable sources** - Where each value comes from
- **Report unresolved variables clearly** - With suggestions
- **Support variable scoping** - Module-level, global, environment

### File: `processors/_variable_processor.py`

```python
"""
Variable replacement with comprehensive tracking.

Variables can come from:
1. config.[env].yaml - Build configuration
2. Environment variables - CDF_* prefixed
3. module.toml - Module-level defaults
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..issues import (
    IssueCollector,
    UnresolvedVariableIssue,
    InvalidVariableValueIssue,
)


# Pattern for template variables: {{ variable_name }}
VARIABLE_PATTERN = re.compile(r"\{\{\s*(\w+(?:\.\w+)*)\s*\}\}")


@dataclass
class VariableReference:
    """A reference to a variable in source content."""
    name: str
    original: str  # Original text (e.g., "{{ my_var }}")
    path: Path
    line: int | None = None


@dataclass  
class VariableValue:
    """A resolved variable value with its source."""
    name: str
    value: Any
    source: str  # "config", "environment", "module"
    source_path: Path | None = None


@dataclass
class VariableScope:
    """Variables scoped to a module or globally."""
    
    global_vars: dict[str, VariableValue] = field(default_factory=dict)
    module_vars: dict[str, dict[str, VariableValue]] = field(default_factory=dict)
    
    def get(self, name: str, module_path: Path | None = None) -> VariableValue | None:
        """Get a variable, checking module scope first then global."""
        if module_path:
            module_key = str(module_path)
            if module_key in self.module_vars:
                if name in self.module_vars[module_key]:
                    return self.module_vars[module_key][name]
        return self.global_vars.get(name)


class VariableProcessor:
    """
    Processes variable replacement in content.
    
    Collects all variable references, resolves them against scoped values,
    and reports unresolved variables with helpful suggestions.
    """
    
    def __init__(self, scope: VariableScope):
        self.scope = scope
        self.issues = IssueCollector()
    
    def process_content(
        self, 
        content: str, 
        source_path: Path,
        module_path: Path | None = None,
    ) -> str:
        """
        Replace variables in content.
        
        Args:
            content: The content with {{ variable }} placeholders
            source_path: Path to the source file (for error reporting)
            module_path: Path to the module (for scoped variable lookup)
        
        Returns:
            Content with variables replaced.
            Unresolved variables are left as-is and reported as issues.
        """
        # Find all variable references
        references = self._find_references(content, source_path)
        
        # Replace each variable
        result = content
        for ref in references:
            value = self.scope.get(ref.name, module_path)
            
            if value is None:
                self.issues.add(UnresolvedVariableIssue(
                    path=source_path,
                    variable_name=ref.name,
                    original_text=ref.original,
                    suggested_location=self._suggest_location(ref.name),
                ))
                continue
            
            # Replace in content
            result = result.replace(ref.original, str(value.value))
        
        return result
    
    def _find_references(self, content: str, path: Path) -> list[VariableReference]:
        """Find all variable references in content."""
        references = []
        for match in VARIABLE_PATTERN.finditer(content):
            references.append(VariableReference(
                name=match.group(1),
                original=match.group(0),
                path=path,
            ))
        return references
    
    def _suggest_location(self, variable_name: str) -> str:
        """Suggest where to define an unresolved variable."""
        # Check if it looks like a module-scoped variable
        if "." in variable_name:
            parts = variable_name.split(".")
            return f"config.[env].yaml under '{parts[0]}:' section"
        return "config.[env].yaml in the variables section"
```

---

## 4. Build Pipeline

### Design Goals

- **Clear stages** - Load → Validate → Transform → Write
- **Fail early** - Stop on fatal errors
- **Continue on warnings** - Collect all non-fatal issues
- **Verbose mode** - Detailed progress for debugging

### File: `build_cmd.py` (updated)

```python
"""
Build command v2 - Type-safe module building with comprehensive error reporting.

The build process has four stages:
1. LOAD - Read modules from filesystem into Pydantic models
2. VALIDATE - Check module structure, variables, dependencies
3. TRANSFORM - Apply variable replacement, run builders
4. WRITE - Create build artifacts in output directory

Issues are collected throughout and reported at the end.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand

from .data_classes import Module, ModuleCollection
from .issues import IssueCollector, BuildError
from .loaders import ModuleLoader
from .processors import VariableProcessor, VariableScope
from .validators import ModuleValidator, DependencyValidator
from .writers import BuildWriter


@dataclass
class BuildResult:
    """Result of a build operation."""
    success: bool
    modules_built: int
    resources_built: int
    issues: IssueCollector
    output_dir: Path


class BuildCommand(ToolkitCommand):
    """
    Build modules into deployable artifacts.
    
    This command processes module directories, applies variable substitution,
    validates resources, and writes the result to a build directory.
    """
    
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
    ) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self.issues = IssueCollector()
    
    def execute(
        self,
        organization_dir: Path,
        build_dir: Path,
        selected: list[str | Path] | None = None,
        build_env: str | None = None,
        no_clean: bool = False,
        verbose: bool = False,
        client: ToolkitClient | None = None,
        on_error: Literal["continue", "raise"] = "continue",
    ) -> BuildResult:
        """
        Execute the build command.
        
        Args:
            organization_dir: Root directory containing modules/
            build_dir: Directory to write build artifacts
            selected: Optional list of modules to build
            build_env: Environment name (e.g., "dev", "prod")
            no_clean: If True, don't clear existing build directory
            verbose: Print detailed progress
            client: Optional CDF client for dependency checking
            on_error: "continue" to collect all errors, "raise" to stop on first
        
        Returns:
            BuildResult with status, counts, and all issues.
        """
        self._print_header(organization_dir, build_env, verbose)
        
        # Stage 1: LOAD
        if verbose:
            print("[bold]Stage 1/4: Loading modules...[/]")
        
        modules, load_config = self._load(organization_dir, build_env, selected)
        
        if self.issues.has_errors and on_error == "raise":
            return self._fail_result(build_dir)
        
        # Stage 2: VALIDATE  
        if verbose:
            print(f"[bold]Stage 2/4: Validating {len(modules)} modules...[/]")
        
        self._validate(modules, load_config, client)
        
        if self.issues.has_errors and on_error == "raise":
            return self._fail_result(build_dir)
        
        # Stage 3: TRANSFORM
        if verbose:
            print("[bold]Stage 3/4: Processing resources...[/]")
        
        built_resources = self._transform(modules, load_config)
        
        if self.issues.has_errors and on_error == "raise":
            return self._fail_result(build_dir)
        
        # Stage 4: WRITE
        if verbose:
            print("[bold]Stage 4/4: Writing build artifacts...[/]")
        
        self._write(built_resources, build_dir, not no_clean)
        
        # Report results
        self._print_summary()
        
        return BuildResult(
            success=not self.issues.has_errors,
            modules_built=len(modules),
            resources_built=len(built_resources),
            issues=self.issues,
            output_dir=build_dir,
        )
    
    def _load(
        self,
        organization_dir: Path,
        build_env: str | None,
        selected: list[str | Path] | None,
    ) -> tuple[ModuleCollection, "BuildConfig"]:
        """Stage 1: Load modules and configuration."""
        # Load modules
        loader = ModuleLoader(organization_dir)
        modules = loader.load(selected)
        self.issues.extend(loader.issues)
        
        # Load build configuration
        config = BuildConfig.load(organization_dir, build_env)
        self.issues.extend(config.issues)
        
        return modules, config
    
    def _validate(
        self,
        modules: ModuleCollection,
        config: "BuildConfig",
        client: ToolkitClient | None,
    ) -> None:
        """Stage 2: Validate modules and dependencies."""
        # Validate module structure
        validator = ModuleValidator()
        for module in modules:
            self.issues.extend(validator.validate(module))
        
        # Validate dependencies (optionally against CDF)
        if client:
            dep_validator = DependencyValidator(client)
            self.issues.extend(dep_validator.validate(modules))
    
    def _transform(
        self,
        modules: ModuleCollection,
        config: "BuildConfig",
    ) -> list["BuiltResource"]:
        """Stage 3: Apply variable replacement and build resources."""
        processor = VariableProcessor(config.variables)
        built = []
        
        for module in modules:
            for folder in module.resource_folders.values():
                for file in folder.files:
                    result = self._process_file(file, module, processor)
                    if result:
                        built.extend(result)
        
        self.issues.extend(processor.issues)
        return built
    
    def _write(
        self,
        resources: list["BuiltResource"],
        build_dir: Path,
        clean: bool,
    ) -> None:
        """Stage 4: Write resources to build directory."""
        writer = BuildWriter(build_dir)
        
        if clean:
            writer.clean()
        
        for resource in resources:
            writer.write(resource)
        
        writer.write_manifest()
    
    def _print_header(
        self,
        organization_dir: Path,
        build_env: str | None,
        verbose: bool,
    ) -> None:
        """Print build header."""
        env_str = f"environment={build_env or 'default'}"
        print(Panel(
            f"[bold]Building[/] {organization_dir}\n{env_str}",
            expand=False,
        ))
    
    def _print_summary(self) -> None:
        """Print build summary with all issues."""
        self.issues.print_summary()
    
    def _fail_result(self, build_dir: Path) -> BuildResult:
        """Create a failed build result."""
        return BuildResult(
            success=False,
            modules_built=0,
            resources_built=0,
            issues=self.issues,
            output_dir=build_dir,
        )
```

---

## 5. Validators (Pluggable System)

### Design Goals

- **Single responsibility** - Each validator checks one aspect
- **Composable** - Run multiple validators in sequence
- **Extensible** - Easy to add new validators
- **Testable** - Each validator can be unit tested

### File: `validators/__init__.py`

```python
"""
Pluggable validation system.

Validators check different aspects of the build:
- ModuleValidator: Module structure and naming
- ResourceValidator: Individual resource syntax and semantics
- DependencyValidator: Cross-resource dependencies
- ConfigValidator: Build configuration correctness

Each validator returns an IssueCollector with any problems found.
"""

from ._base import Validator
from ._module import ModuleValidator
from ._resource import ResourceValidator  
from ._dependency import DependencyValidator
from ._config import ConfigValidator

__all__ = [
    "Validator",
    "ModuleValidator",
    "ResourceValidator",
    "DependencyValidator", 
    "ConfigValidator",
]
```

### File: `validators/_base.py`

```python
"""Base validator interface."""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from ..issues import IssueCollector

T = TypeVar("T")


class Validator(ABC, Generic[T]):
    """
    Base class for validators.
    
    Validators inspect an object and return issues found.
    They should not modify the object.
    """
    
    @abstractmethod
    def validate(self, item: T) -> IssueCollector:
        """
        Validate an item and return any issues.
        
        Args:
            item: The item to validate
            
        Returns:
            IssueCollector with any issues found (may be empty)
        """
        ...


class CompositeValidator(Validator[T]):
    """Runs multiple validators in sequence."""
    
    def __init__(self, validators: list[Validator[T]]):
        self._validators = validators
    
    def validate(self, item: T) -> IssueCollector:
        issues = IssueCollector()
        for validator in self._validators:
            issues.extend(validator.validate(item))
        return issues
```

### File: `validators/_module.py`

```python
"""Module structure validation."""

from pathlib import Path

from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME

from ..data_classes import Module
from ..issues import (
    IssueCollector,
    ModuleStructureIssue,
    UnrecognizedResourceFolderIssue,
)
from ._base import Validator


class ModuleValidator(Validator[Module]):
    """
    Validates module structure.
    
    Checks:
    - Module has valid resource folders
    - Resource folders contain expected file types
    - Module naming conventions
    - module.toml format (if present)
    """
    
    def validate(self, module: Module) -> IssueCollector:
        issues = IssueCollector()
        
        # Check module has resources
        if not module.resource_folders:
            issues.add(ModuleStructureIssue(
                path=module.path,
                reason="Module contains no resource folders",
            ))
            return issues
        
        # Validate each resource folder
        for folder_name, folder in module.resource_folders.items():
            folder_issues = self._validate_resource_folder(folder_name, folder, module)
            issues.extend(folder_issues)
        
        # Validate module naming
        naming_issues = self._validate_naming(module)
        issues.extend(naming_issues)
        
        return issues
    
    def _validate_resource_folder(
        self,
        name: str,
        folder: "ResourceFolder",
        module: Module,
    ) -> IssueCollector:
        """Validate a single resource folder."""
        issues = IssueCollector()
        
        # Check folder has files
        if folder.file_count == 0:
            issues.add(ModuleStructureIssue(
                path=folder.path,
                reason=f"Resource folder '{name}' contains no YAML files",
            ))
        
        return issues
    
    def _validate_naming(self, module: Module) -> IssueCollector:
        """Validate module naming conventions."""
        issues = IssueCollector()
        
        # Check for recommended naming patterns
        name = module.name
        
        # Warn about spaces or special characters
        if " " in name or not name.replace("_", "").replace("-", "").isalnum():
            issues.add(ModuleStructureIssue(
                path=module.path,
                reason=f"Module name '{name}' contains special characters. "
                       f"Use only alphanumeric characters, hyphens, and underscores.",
            ))
        
        return issues
```

---

## 6. Issue Codes Reference

### Module Issues (MOD_xxx)

| Code    | Severity | Description                          |
| ------- | -------- | ------------------------------------ |
| MOD_001 | Error    | Module directory not found           |
| MOD_002 | Error    | No valid resource folders in module  |
| MOD_003 | Warning  | Unrecognized resource folder         |
| MOD_004 | Warning  | Disabled resource folder             |
| MOD_005 | Error    | Duplicate module names               |
| MOD_006 | Hint     | Module naming conventions            |

### Variable Issues (VAR_xxx)

| Code    | Severity | Description                                           |
| ------- | -------- | ----------------------------------------------------- |
| VAR_001 | Warning  | Unresolved variable                                   |
| VAR_002 | Error    | Invalid variable value type                           |
| VAR_003 | Error    | Missing required variable                             |
| VAR_004 | Hint     | Unused variable defined                               |
| VAR_005 | Warning  | Template variable not replaced (e.g., `<change_me>`)  |

### Resource Issues (RES_xxx)

| Code    | Severity | Description                        |
| ------- | -------- | ---------------------------------- |
| RES_001 | Error    | YAML syntax error                  |
| RES_002 | Error    | Missing required field             |
| RES_003 | Warning  | Unknown field (will be ignored)    |
| RES_004 | Warning  | Duplicate resource identifier      |
| RES_005 | Warning  | Invalid resource reference         |

### Dependency Issues (DEP_xxx)

| Code    | Severity | Description                    |
| ------- | -------- | ------------------------------ |
| DEP_001 | Warning  | Missing dependency in build    |
| DEP_002 | Warning  | Missing dependency in CDF      |
| DEP_003 | Hint     | Circular dependency detected   |

### Configuration Issues (CFG_xxx)

| Code    | Severity | Description              |
| ------- | -------- | ------------------------ |
| CFG_001 | Error    | Config file not found    |
| CFG_002 | Error    | Invalid config format    |
| CFG_003 | Warning  | Unknown config option    |
| CFG_004 | Hint     | Deprecated config option |

---

## 7. Directory Structure

```text
cognite_toolkit/_cdf_tk/commands/build_v2/
├── __init__.py
├── build_cmd.py              # Main build command
├── build_config.py           # Build configuration loading
│
├── data_classes/
│   ├── __init__.py
│   ├── _module.py           # Module, ResourceFolder, ModuleCollection
│   ├── _resource.py         # BuiltResource, ResourceFile
│   └── _result.py           # BuildResult
│
├── loaders/
│   ├── __init__.py
│   ├── _module_loader.py    # Load modules from filesystem
│   ├── _config_loader.py    # Load build configuration
│   └── _resource_loader.py  # Load individual resource files
│
├── processors/
│   ├── __init__.py
│   ├── _variable_processor.py  # Variable replacement
│   └── _resource_processor.py  # Resource transformation
│
├── validators/
│   ├── __init__.py
│   ├── _base.py             # Validator interface
│   ├── _module.py           # Module structure validation
│   ├── _resource.py         # Resource syntax/semantics
│   ├── _dependency.py       # Cross-resource dependencies
│   └── _config.py           # Configuration validation
│
├── writers/
│   ├── __init__.py
│   └── _build_writer.py     # Write to build directory
│
└── issues/
    ├── __init__.py
    ├── _base.py             # BuildIssue, BuildError, BuildWarning
    ├── _collection.py       # IssueCollector
    ├── _module_issues.py    # MOD_xxx issues
    ├── _variable_issues.py  # VAR_xxx issues
    ├── _resource_issues.py  # RES_xxx issues
    └── _dependency_issues.py # DEP_xxx issues
```

---

## 8. Migration Strategy

### Phase 1: Foundation (Issues & Data Classes)

1. Create issue system (`issues/` directory)
2. Create new Pydantic data classes
3. Unit tests for both

### Phase 2: Loading

1. Implement ModuleLoader with full error handling
2. Update existing Module loading to use new system
3. Integration tests for loading

### Phase 3: Processing

1. Implement VariableProcessor
2. Implement ResourceProcessor
3. Integration tests

### Phase 4: Validation

1. Implement all validators
2. Wire into build pipeline
3. End-to-end tests

### Phase 5: Integration

1. Replace old build command calls
2. Deprecate old build_cmd.py
3. Update CLI to use new command

---

## 9. Testing Strategy

### Unit Tests

```python
# tests/test_unit/test_build_v2/test_issues.py
def test_issue_code_uniqueness():
    """All issue codes must be unique."""
    
def test_issue_formatting():
    """Issue messages format correctly."""

def test_issue_collector_grouping():
    """Issues group correctly by category."""
```

### Integration Tests

```python
# tests/test_integration/test_build_v2/test_module_loading.py
def test_load_valid_module():
    """Valid modules load without issues."""

def test_load_module_with_unrecognized_folder():
    """Unrecognized folders produce MOD_003 warning."""

def test_load_module_missing_resources():
    """Empty modules produce MOD_002 error."""
```

### End-to-End Tests

```python
# tests/test_integration/test_build_v2/test_build_command.py
def test_full_build_success():
    """Complete build with no issues."""

def test_build_with_unresolved_variables():
    """Build reports VAR_001 for unresolved variables."""

def test_build_reports_all_issues():
    """Build collects all issues when on_error='continue'."""
```

---

## 10. Open Questions

1. **Backward compatibility**: How long to support old issue format?
2. **Issue persistence**: Should issues be written to a file for CI/CD?
3. **Issue suppression**: Should users be able to suppress specific codes?
4. **Performance**: Should we parallelize module loading?

---

## Summary

This plan provides:

- **Clear issue system** with codes, severity, and helpful messages
- **Pydantic-based module loading** with comprehensive validation
- **Pluggable validators** for extensibility
- **Four-stage build pipeline** that's easy to understand and debug
- **Migration path** from the existing implementation

The focus on readable, maintainable code following the Gemini style guide ensures
the codebase remains approachable for future development.
