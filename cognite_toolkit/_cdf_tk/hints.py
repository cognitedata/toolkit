from __future__ import annotations

import sys
from abc import abstractmethod
from pathlib import Path
from typing import Any

from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml

from .constants import COGNITE_MODULES, CUSTOM_MODULES, HINT_LEAD_TEXT, MODULES, ROOT_MODULES, URL
from .exceptions import ToolkitFileNotFoundError, ToolkitNotADirectoryError
from .loaders import LOADER_BY_FOLDER_NAME
from .tk_warnings import MediumSeverityWarning
from .utils import find_directory_with_subdirectories

CDF_TOML = CDFToml.load(Path.cwd())


class Hint:
    _indent = " " * 5
    _lead_text = HINT_LEAD_TEXT

    @classmethod
    @abstractmethod
    def _short(cls) -> str: ...

    @classmethod
    @abstractmethod
    def long(cls, *args: Any, **kwargs: Any) -> list[str]: ...

    @classmethod
    def short(cls) -> str:
        return f"{cls._lead_text}{cls._short()}"

    @classmethod
    def _to_hint(cls, lines: list[str]) -> str:
        return cls._lead_text + f"\n{cls._indent}".join(lines)

    @classmethod
    def link(cls, url: str, text: str | None = None) -> str:
        return f"[blue][link={url}]{text or url}[/link][/blue]"


class ModuleDefinition(Hint):
    @classmethod
    def _short(cls) -> str:
        return f"Available resource directories are {sorted(LOADER_BY_FOLDER_NAME)}. {cls.link(URL.configs)} to learn more."

    @classmethod
    def long(cls, missing_modules: set[str | Path] | None = None, organization_dir: Path | None = None) -> str:  # type: ignore[override]
        lines = [
            "A module is a directory with one or more resource directories in it.",
            f"Available resource directories are {sorted(LOADER_BY_FOLDER_NAME)}",
            f"{cls.link(URL.configs)} to learn more",
        ]
        if missing_modules and organization_dir:
            found_directory, subdirectories = find_directory_with_subdirectories(
                next((m for m in missing_modules if isinstance(m, str)), None), organization_dir
            )
            if found_directory:
                lines += [
                    f"For example, the directory {found_directory.as_posix()!r} is not a module, as none of its",
                    f"subdirectories are resource directories. The subdirectories found are: {subdirectories}",
                ]
        return cls._to_hint(lines)


def verify_module_directory(organization_dir: Path, build_env_name: str | None) -> None:
    from .data_classes import BuildConfigYAML

    config_file = BuildConfigYAML.get_filename(build_env_name or "MISSING")

    has_config_yaml = CDF_TOML.cdf.has_user_set_default_env and build_env_name is not None
    if organization_dir != Path.cwd():
        if has_config_yaml:
            content = f"  ┣ {MODULES}/\n  ┗ {config_file}\n"
        else:
            content = f"  ┗ {MODULES}\n"

        panel = Panel(
            f"Toolkit expects the following structure:\n{organization_dir!s}/\n{content}",
            expand=False,
        )
    else:
        if build_env_name:
            content = f"\n  {MODULES}/\n  {config_file}\n"
        else:
            content = f"\n  {MODULES}\n"

        panel = Panel(
            f"Toolkit expects the following structure:{content}",
            expand=False,
        )
    if not organization_dir.is_dir():
        print(panel)
        raise ToolkitNotADirectoryError(f"{organization_dir.as_posix()!r} is not a directory.")

    root_modules = [
        module_dir for root_module in ROOT_MODULES if (module_dir := organization_dir / root_module).exists()
    ]
    if deprecated_modules := [
        root_module for root_module in [CUSTOM_MODULES, COGNITE_MODULES] if (organization_dir / root_module).exists()
    ]:
        if (organization_dir / MODULES).exists():
            suffix = f"Move the modules into {MODULES}/ directory."
        elif len(deprecated_modules) == 1:
            suffix = f"Rename the directory to {MODULES}/."
        else:
            suffix = f"Combine the two into a new {MODULES}/ directory."
        prefix = "Directories" if len(deprecated_modules) > 1 else "Directory"
        verb = "are" if len(deprecated_modules) > 1 else "is"
        MediumSeverityWarning(
            f"{prefix} {', '.join(deprecated_modules)} {verb} deprecated and will be removed in 0.4.0. {suffix}"
        ).print_warning()

    config_path = organization_dir / config_file
    if root_modules and (config_path.is_file() or not has_config_yaml):
        return
    if root_modules or (config_path.is_file() or not has_config_yaml):
        print(panel)
        if not root_modules:
            raise ToolkitNotADirectoryError(
                f"Could not find the {(organization_dir / MODULES).as_posix()!r} directory."
            )
        if not has_config_yaml:
            return
        else:
            raise ToolkitFileNotFoundError(f"Could not find the {config_path.as_posix()!r} file.")

    # Search for the modules directory
    candidate_org = next(
        (
            path
            for path in organization_dir.iterdir()
            if path.is_dir() and any((path / sub).exists() for sub in ROOT_MODULES)
        ),
        None,
    )
    print(panel)
    if candidate_org is not None:
        if candidate_org.is_relative_to(Path.cwd()):
            candidate_rel = candidate_org.relative_to(Path.cwd())
        else:
            candidate_rel = candidate_org
        user_arg = sys.argv
        suggestion = ["cdf"]
        skip_next = False
        found = False
        for arg in user_arg[1:]:
            if arg in ("-o", "--organization-dir"):
                suggestion.append(f"{arg} {candidate_rel}")
                skip_next = True
                found = True
                continue
            if skip_next:
                skip_next = False
                continue
            suggestion.append(arg)
        if not found:
            suggestion.append(f"-o {candidate_rel}")

        print(f"{Hint._lead_text} Did you mean to use the command: '{' '.join(suggestion)}'?")
        cdf_toml = CDFToml.load()
        if not cdf_toml.cdf.has_user_set_default_org:
            print(
                f"{Hint._lead_text} You can specify a 'default_organization_dir = ...' in the 'cdf' section of your "
                f"'{CDFToml.file_name}' file to avoid using the -o/--organization-dir argument"
            )

    raise ToolkitNotADirectoryError(f"Could not find the {(organization_dir / MODULES).as_posix()!r} directory.")
