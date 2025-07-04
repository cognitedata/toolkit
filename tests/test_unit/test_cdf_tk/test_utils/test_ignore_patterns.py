import tempfile
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.utils.ignore_patterns import (
    ToolkitIgnoreParser,
    ToolkitIgnorePattern,
    create_ignore_parser_for_module,
    find_toolkitignore_files,
)


class TestToolkitIgnorePattern:
    """Test the ToolkitIgnorePattern class."""

    def test_simple_pattern_matching(self):
        """Test basic pattern matching."""
        pattern = ToolkitIgnorePattern("node_modules")
        assert pattern.matches(Path("node_modules"), is_directory=True)
        assert pattern.matches(Path("some/path/node_modules"), is_directory=True)
        assert not pattern.matches(Path("node_modules_backup"), is_directory=True)

    def test_wildcard_pattern_matching(self):
        """Test wildcard pattern matching."""
        pattern = ToolkitIgnorePattern("*.tmp")
        assert pattern.matches(Path("temp.tmp"), is_directory=False)
        assert pattern.matches(Path("some/path/temp.tmp"), is_directory=False)
        assert not pattern.matches(Path("temp.txt"), is_directory=False)

    def test_directory_only_pattern(self):
        """Test directory-only patterns ending with /."""
        pattern = ToolkitIgnorePattern("build/")
        assert pattern.matches(Path("build"), is_directory=True)
        assert not pattern.matches(Path("build"), is_directory=False)
        assert not pattern.matches(Path("build.txt"), is_directory=False)

    def test_negation_pattern(self):
        """Test negation patterns starting with !."""
        pattern = ToolkitIgnorePattern("!important.txt")
        assert pattern.is_negation
        assert pattern.matches(Path("important.txt"), is_directory=False)


class TestToolkitIgnoreParser:
    """Test the ToolkitIgnoreParser class."""

    def test_empty_parser(self):
        """Test empty parser behavior."""
        parser = ToolkitIgnoreParser()
        assert not parser.is_ignored(Path("anything"), is_directory=True)

    def test_single_pattern_ignore(self):
        """Test ignoring with a single pattern."""
        pattern = ToolkitIgnorePattern("node_modules")
        parser = ToolkitIgnoreParser([pattern])
        
        assert parser.is_ignored(Path("node_modules"), is_directory=True)
        assert not parser.is_ignored(Path("src"), is_directory=True)

    def test_negation_pattern_override(self):
        """Test that negation patterns override previous ignore patterns."""
        patterns = [
            ToolkitIgnorePattern("*.tmp"),
            ToolkitIgnorePattern("!important.tmp")
        ]
        parser = ToolkitIgnoreParser(patterns)
        
        assert parser.is_ignored(Path("temp.tmp"), is_directory=False)
        assert not parser.is_ignored(Path("important.tmp"), is_directory=False)

    def test_from_file_content(self):
        """Test creating parser from file content."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toolkitignore', delete=False) as f:
            f.write("# This is a comment\n")
            f.write("node_modules\n")
            f.write("*.tmp\n")
            f.write("!important.tmp\n")
            f.write("\n")  # Empty line
            f.write("build/\n")
            f.flush()
            
            parser = ToolkitIgnoreParser.from_file(Path(f.name))
            
            assert len(parser.patterns) == 4  # Excluding comment and empty line
            assert parser.is_ignored(Path("node_modules"), is_directory=True)
            assert parser.is_ignored(Path("temp.tmp"), is_directory=False)
            assert not parser.is_ignored(Path("important.tmp"), is_directory=False)
            assert parser.is_ignored(Path("build"), is_directory=True)

    def test_from_nonexistent_file(self):
        """Test creating parser from non-existent file."""
        parser = ToolkitIgnoreParser.from_file(Path("nonexistent.toolkitignore"))
        assert len(parser.patterns) == 0


class TestIntegrationFunctions:
    """Test the integration functions."""

    def test_create_ignore_parser_for_module(self):
        """Test creating ignore parser for a module."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a .toolkitignore file
            ignore_file = temp_path / ".toolkitignore"
            ignore_file.write_text("node_modules\n*.tmp\n")
            
            parser = create_ignore_parser_for_module(temp_path)
            
            assert len(parser.patterns) == 2
            assert parser.is_ignored(Path("node_modules"), is_directory=True)
            assert parser.is_ignored(Path("temp.tmp"), is_directory=False) 