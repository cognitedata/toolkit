import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleLocation
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml


class TestModuleLocationWithToolkitIgnore:
    """Test ModuleLocation integration with .toolkitignore functionality."""

    def test_module_ignores_directories_from_toolkitignore(self):
        """Test that ModuleLocation respects .toolkitignore patterns."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create module directory structure
            module_dir = temp_path / "test_module"
            module_dir.mkdir()
            
            # Create .toolkitignore file
            ignore_file = module_dir / ".toolkitignore"
            ignore_file.write_text("node_modules\n*.tmp\ntemp_files/\n")
            
            # Create directories - some should be ignored
            (module_dir / "auth").mkdir()  # Valid resource directory
            (module_dir / "data_models").mkdir()  # Valid resource directory
            (module_dir / "node_modules").mkdir()  # Should be ignored
            (module_dir / "temp_files").mkdir()  # Should be ignored
            (module_dir / "custom_folder").mkdir()  # Should be flagged as invalid
            
            # Create some files
            (module_dir / "auth" / "groups.yaml").touch()
            (module_dir / "data_models" / "model.yaml").touch()
            (module_dir / "node_modules" / "package.json").touch()
            (module_dir / "temp_files" / "cache.txt").touch()
            (module_dir / "custom_folder" / "file.txt").touch()
            (module_dir / "temp.tmp").touch()  # Should be ignored
            
            # Create source_paths list (simulating file discovery)
            source_paths = []
            for file_path in module_dir.rglob("*"):
                if file_path.is_file():
                    source_paths.append(file_path)
            
            # Create ModuleLocation instance
            module_location = ModuleLocation(
                dir=module_dir,
                source_absolute_path=temp_path,
                source_paths=source_paths,
                is_selected=True,
                definition=None
            )
            
            # Test that ignored directories are not flagged as invalid
            not_resource_dirs = module_location.not_resource_directories
            
            # Should only flag custom_folder as invalid (not ignored directories)
            assert "custom_folder" in not_resource_dirs
            assert "node_modules" not in not_resource_dirs  # Should be ignored
            assert "temp_files" not in not_resource_dirs  # Should be ignored
            assert "auth" not in not_resource_dirs  # Valid resource directory
            assert "data_models" not in not_resource_dirs  # Valid resource directory

    def test_module_with_nested_toolkitignore_files(self):
        """Test ModuleLocation with nested .toolkitignore files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create root .toolkitignore
            root_ignore = temp_path / ".toolkitignore"
            root_ignore.write_text("*.log\n__pycache__\n")
            
            # Create module directory
            module_dir = temp_path / "modules" / "test_module"
            module_dir.mkdir(parents=True)
            
            # Create module .toolkitignore
            module_ignore = module_dir / ".toolkitignore"
            module_ignore.write_text("temp_data\n*.tmp\n")
            
            # Create directories
            (module_dir / "auth").mkdir()
            (module_dir / "data_models").mkdir()
            (module_dir / "temp_data").mkdir()  # Should be ignored by module ignore
            (module_dir / "__pycache__").mkdir()  # Should be ignored by root ignore
            (module_dir / "invalid_dir").mkdir()  # Should be flagged as invalid
            
            # Create files
            (module_dir / "auth" / "groups.yaml").touch()
            (module_dir / "data_models" / "model.yaml").touch()
            (module_dir / "temp_data" / "cache.txt").touch()
            (module_dir / "__pycache__" / "cache.pyc").touch()
            (module_dir / "invalid_dir" / "file.txt").touch()
            (module_dir / "app.log").touch()  # Should be ignored by root ignore
            (module_dir / "temp.tmp").touch()  # Should be ignored by module ignore
            
            # Create source_paths list
            source_paths = []
            for file_path in module_dir.rglob("*"):
                if file_path.is_file():
                    source_paths.append(file_path)
            
            # Create ModuleLocation instance
            module_location = ModuleLocation(
                dir=module_dir,
                source_absolute_path=temp_path,
                source_paths=source_paths,
                is_selected=True,
                definition=None
            )
            
            # Test that ignored directories are not flagged as invalid
            not_resource_dirs = module_location.not_resource_directories
            
            # Should only flag invalid_dir as invalid
            assert "invalid_dir" in not_resource_dirs
            assert "temp_data" not in not_resource_dirs  # Ignored by module ignore
            assert "__pycache__" not in not_resource_dirs  # Ignored by root ignore
            assert "auth" not in not_resource_dirs  # Valid resource directory
            assert "data_models" not in not_resource_dirs  # Valid resource directory

    def test_module_with_negation_patterns(self):
        """Test ModuleLocation with negation patterns in .toolkitignore."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create module directory
            module_dir = temp_path / "test_module"
            module_dir.mkdir()
            
            # Create .toolkitignore with negation
            ignore_file = module_dir / ".toolkitignore"
            ignore_file.write_text("temp_*\n!temp_important\n")
            
            # Create directories
            (module_dir / "auth").mkdir()
            (module_dir / "temp_cache").mkdir()  # Should be ignored
            (module_dir / "temp_important").mkdir()  # Should NOT be ignored (negation)
            (module_dir / "temp_logs").mkdir()  # Should be ignored
            (module_dir / "other_invalid").mkdir()  # Should be flagged as invalid
            
            # Create files
            (module_dir / "auth" / "groups.yaml").touch()
            (module_dir / "temp_cache" / "cache.txt").touch()
            (module_dir / "temp_important" / "important.txt").touch()
            (module_dir / "temp_logs" / "log.txt").touch()
            (module_dir / "other_invalid" / "file.txt").touch()
            
            # Create source_paths list
            source_paths = []
            for file_path in module_dir.rglob("*"):
                if file_path.is_file():
                    source_paths.append(file_path)
            
            # Create ModuleLocation instance
            module_location = ModuleLocation(
                dir=module_dir,
                source_absolute_path=temp_path,
                source_paths=source_paths,
                is_selected=True,
                definition=None
            )
            
            # Test that ignored directories are not flagged as invalid
            not_resource_dirs = module_location.not_resource_directories
            
            # Should flag temp_important and other_invalid as invalid
            assert "temp_important" in not_resource_dirs  # Not ignored due to negation
            assert "other_invalid" in not_resource_dirs  # Not ignored, so flagged as invalid
            assert "temp_cache" not in not_resource_dirs  # Ignored by pattern
            assert "temp_logs" not in not_resource_dirs  # Ignored by pattern
            assert "auth" not in not_resource_dirs  # Valid resource directory

    def test_module_without_toolkitignore(self):
        """Test ModuleLocation behavior when no .toolkitignore file exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create module directory structure without .toolkitignore
            module_dir = temp_path / "test_module"
            module_dir.mkdir()
            
            # Create directories
            (module_dir / "auth").mkdir()
            (module_dir / "data_models").mkdir()
            (module_dir / "invalid_dir").mkdir()
            
            # Create files
            (module_dir / "auth" / "groups.yaml").touch()
            (module_dir / "data_models" / "model.yaml").touch()
            (module_dir / "invalid_dir" / "file.txt").touch()
            
            # Create source_paths list
            source_paths = []
            for file_path in module_dir.rglob("*"):
                if file_path.is_file():
                    source_paths.append(file_path)
            
            # Create ModuleLocation instance
            module_location = ModuleLocation(
                dir=module_dir,
                source_absolute_path=temp_path,
                source_paths=source_paths,
                is_selected=True,
                definition=None
            )
            
            # Test that all non-resource directories are flagged as invalid
            not_resource_dirs = module_location.not_resource_directories
            
            # Should flag invalid_dir as invalid (no ignore file to prevent this)
            assert "invalid_dir" in not_resource_dirs
            assert "auth" not in not_resource_dirs  # Valid resource directory
            assert "data_models" not in not_resource_dirs  # Valid resource directory

    def test_empty_toolkitignore_file(self):
        """Test ModuleLocation with empty .toolkitignore file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create module directory
            module_dir = temp_path / "test_module"
            module_dir.mkdir()
            
            # Create empty .toolkitignore file
            ignore_file = module_dir / ".toolkitignore"
            ignore_file.write_text("")
            
            # Create directories
            (module_dir / "auth").mkdir()
            (module_dir / "invalid_dir").mkdir()
            
            # Create files
            (module_dir / "auth" / "groups.yaml").touch()
            (module_dir / "invalid_dir" / "file.txt").touch()
            
            # Create source_paths list
            source_paths = []
            for file_path in module_dir.rglob("*"):
                if file_path.is_file():
                    source_paths.append(file_path)
            
            # Create ModuleLocation instance
            module_location = ModuleLocation(
                dir=module_dir,
                source_absolute_path=temp_path,
                source_paths=source_paths,
                is_selected=True,
                definition=None
            )
            
            # Test that empty ignore file doesn't prevent flagging
            not_resource_dirs = module_location.not_resource_directories
            
            # Should still flag invalid_dir as invalid
            assert "invalid_dir" in not_resource_dirs
            assert "auth" not in not_resource_dirs  # Valid resource directory 