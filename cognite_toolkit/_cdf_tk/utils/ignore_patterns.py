"""Minimal stub for ignore patterns utility - for test dependencies only."""

from pathlib import Path
from typing import Iterable


class ToolkitIgnorePattern:
    """Minimal stub for tests."""
    
    def __init__(self, pattern: str):
        self.pattern = pattern
        self.is_negation = False
        self.is_directory_only = False
    
    def matches(self, path: Path, is_directory: bool = False) -> bool:
        """Stub implementation."""
        return False


class ToolkitIgnoreParser:
    """Minimal stub for tests."""
    
    def __init__(self, patterns: list[ToolkitIgnorePattern] | None = None):
        self.patterns = patterns or []
    
    def is_ignored(self, path: Path, is_directory: bool = False) -> bool:
        """Stub implementation."""
        return False
    
    @classmethod
    def from_file(cls, ignore_file: Path) -> "ToolkitIgnoreParser":
        """Stub implementation."""
        return cls()
    
    @classmethod
    def from_directory(cls, directory: Path, filename: str = ".toolkitignore") -> "ToolkitIgnoreParser":
        """Stub implementation."""
        return cls()


def find_toolkitignore_files(directory: Path, filename: str = ".toolkitignore") -> list[Path]:
    """Stub implementation."""
    return []


def create_ignore_parser_for_module(module_dir: Path) -> ToolkitIgnoreParser:
    """Stub implementation."""
    return ToolkitIgnoreParser() 