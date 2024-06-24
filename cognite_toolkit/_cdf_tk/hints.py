from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Any

from .constants import URL
from .loaders import LOADER_BY_FOLDER_NAME
from .utils import find_directory_with_subdirectories


class Hint:
    _indent = " " * 5
    _lead_text = "[bold blue]HINT[/bold blue] "

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
    def _link(cls, url: str, text: str | None = None) -> str:
        return f"[blue][link={url}]{text or url}[/link][/blue]"


class ModuleDefinition(Hint):
    @classmethod
    def _short(cls) -> str:
        return f"Available resource directories are {sorted(LOADER_BY_FOLDER_NAME)}. {cls._link(URL.configs)} to learn more."

    @classmethod
    def long(cls, missing_modules: set[str | tuple[str, ...]] | None = None, source_dir: Path | None = None) -> str:  # type: ignore[override]
        lines = [
            "A module is a directory with one or more resource directories in it.",
            f"Available resource directories are {sorted(LOADER_BY_FOLDER_NAME)}",
            f"{cls._link(URL.configs)} to learn more",
        ]
        if missing_modules and source_dir:
            found_directory, subdirectories = find_directory_with_subdirectories(
                next((m for m in missing_modules if isinstance(m, str)), None), source_dir
            )
            if found_directory:
                lines += [
                    f"For example, the directory {found_directory.as_posix()!r} is not a module, as none of its",
                    f"subdirectories are resource directories. The subdirectories found are: {subdirectories}",
                ]
        return cls._to_hint(lines)
