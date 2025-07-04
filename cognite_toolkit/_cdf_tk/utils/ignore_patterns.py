"""Utility for parsing and matching .toolkitignore patterns.

This module provides functionality to parse .toolkitignore files and match
file/directory paths against gitignore-style patterns.
"""

import fnmatch
from pathlib import Path
from typing import Iterable


class ToolkitIgnorePattern:
    """Represents a single ignore pattern from a .toolkitignore file."""
    
    def __init__(self, pattern: str):
        self.original_pattern = pattern
        self.is_negation = False
        self.is_directory_only = False
        self.pattern = self._normalize_pattern(pattern)
    
    def _normalize_pattern(self, pattern: str) -> str:
        """Normalize a gitignore-style pattern for matching."""
        # Remove leading/trailing whitespace
        pattern = pattern.strip()
        
        # Handle negation
        if pattern.startswith("!"):
            self.is_negation = True
            pattern = pattern[1:]
        
        # Handle directory-only patterns
        if pattern.endswith("/"):
            self.is_directory_only = True
            pattern = pattern[:-1]
        
        # Handle leading slash (absolute path from root)
        if pattern.startswith("/"):
            pattern = pattern[1:]
        
        return pattern
    
    def matches(self, path: Path, is_directory: bool = False) -> bool:
        """Check if this pattern matches the given path."""
        if self.is_directory_only and not is_directory:
            return False
        
        # Convert path to string for matching
        path_str = path.as_posix()
        
        # Try exact match first
        if fnmatch.fnmatch(path_str, self.pattern):
            return True
        
        # Try matching against any parent directory
        if "/" not in self.pattern:
            # Simple filename pattern - check if it matches any part of the path
            return any(fnmatch.fnmatch(part, self.pattern) for part in path.parts)
        
        # Pattern contains directories - match against full path
        return fnmatch.fnmatch(path_str, self.pattern)


class ToolkitIgnoreParser:
    """Parser for .toolkitignore files."""
    
    def __init__(self, patterns: list[ToolkitIgnorePattern] | None = None):
        self.patterns = patterns or []
    
    @classmethod
    def from_file(cls, ignore_file: Path) -> "ToolkitIgnoreParser":
        """Create a parser from a .toolkitignore file."""
        patterns = []
        
        if not ignore_file.exists():
            return cls(patterns)
        
        try:
            content = ignore_file.read_text(encoding="utf-8")
            for line in content.splitlines():
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue
                
                pattern = ToolkitIgnorePattern(line)
                patterns.append(pattern)
        
        except (OSError, UnicodeDecodeError):
            # If we can't read the file, just return empty parser
            pass
        
        return cls(patterns)
    
    @classmethod
    def from_directory(cls, directory: Path, filename: str = ".toolkitignore") -> "ToolkitIgnoreParser":
        """Create a parser by looking for ignore files in directory and parent directories."""
        patterns = []
        
        # Start from the given directory and walk up to find ignore files
        current_dir = directory
        while current_dir != current_dir.parent:
            ignore_file = current_dir / filename
            if ignore_file.exists():
                parser = cls.from_file(ignore_file)
                patterns.extend(parser.patterns)
            current_dir = current_dir.parent
        
        return cls(patterns)
    
    def is_ignored(self, path: Path, is_directory: bool = False) -> bool:
        """Check if a path should be ignored based on the loaded patterns."""
        # Start with not ignored
        ignored = False
        
        # Apply patterns in order
        for pattern in self.patterns:
            if pattern.matches(path, is_directory):
                if pattern.is_negation:
                    ignored = False
                else:
                    ignored = True
        
        return ignored
    
    def filter_paths(self, paths: Iterable[Path], check_directory: bool = True) -> list[Path]:
        """Filter a list of paths, removing ignored ones."""
        filtered = []
        
        for path in paths:
            is_dir = path.is_dir() if check_directory else False
            if not self.is_ignored(path, is_dir):
                filtered.append(path)
        
        return filtered


def find_toolkitignore_files(directory: Path, filename: str = ".toolkitignore") -> list[Path]:
    """Find all .toolkitignore files from directory up to root."""
    ignore_files = []
    current_dir = directory
    
    while current_dir != current_dir.parent:
        ignore_file = current_dir / filename
        if ignore_file.exists():
            ignore_files.append(ignore_file)
        current_dir = current_dir.parent
    
    return ignore_files


def create_ignore_parser_for_module(module_dir: Path) -> ToolkitIgnoreParser:
    """Create an ignore parser for a specific module directory."""
    return ToolkitIgnoreParser.from_directory(module_dir) 