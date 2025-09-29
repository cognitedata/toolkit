from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.data_classes._packages import Package, Packages


class TestPackages:
    @pytest.fixture(autouse=True)
    def builtin_packages(self) -> Packages:
        res = Packages.load(BUILTIN_MODULES_PATH)
        assert res is not None
        assert len(res) >= 5

        return res

    def test_load(self, builtin_packages: Packages) -> None:
        assert "infield" in builtin_packages
        infield = builtin_packages["infield"]
        assert len(infield.modules) > 0

    @pytest.mark.parametrize(
        "package_name, expected_module_names",
        [
            ("infield", {"cdf_apm_base", "cdf_infield_common", "cdf_infield_location", "cdf_infield_second_location"}),
            ("inrobot", {"cdf_apm_base", "cdf_inrobot_common", "cdf_inrobot_location"}),
            (
                "quickstart",
                {
                    "cdf_ingestion",
                    "cdf_connection_sql",
                    "cdf_p_and_id_parser",
                    "cdf_search",
                    "cdf_process_industry_extension",
                    "cdf_pi",
                    "cdf_sap_assets",
                    "cdf_sap_events",
                    "cdf_sharepoint",
                },
            ),
        ],
    )
    def test_load_module_duplication(
        self, builtin_packages: Packages, package_name: str, expected_module_names: list[str]
    ) -> None:
        # Assert that the fixture provided packages (basic sanity check)

        # Access the specific package using the parameterized package_name
        package = builtin_packages[package_name]

        assert package is not None
        assert package.module_names == expected_module_names

    def test_package_creation_with_id(self) -> None:
        """Test creating a package with an ID."""
        package = Package(
            name="test_package",
            title="Test Package",
            description="A test package",
            id="test-package-123",
        )
        
        assert package.name == "test_package"
        assert package.title == "Test Package"
        assert package.description == "A test package"
        assert package.id == "test-package-123"
        assert package.can_cherry_pick is True
        assert len(package.modules) == 0

    def test_package_creation_without_id(self) -> None:
        """Test creating a package without an ID (backward compatibility)."""
        package = Package(
            name="test_package",
            title="Test Package",
            description="A test package",
        )
        
        assert package.name == "test_package"
        assert package.title == "Test Package"
        assert package.description == "A test package"
        assert package.id is None
        assert package.can_cherry_pick is True

    def test_package_load_with_id(self) -> None:
        """Test loading a package from dictionary with ID."""
        package_definition = {
            "title": "Test Package",
            "description": "A test package",
            "id": "test-package-123",
            "canCherryPick": False,
        }
        
        package = Package.load("test_package", package_definition)
        
        assert package.name == "test_package"
        assert package.title == "Test Package"
        assert package.description == "A test package"
        assert package.id == "test-package-123"
        assert package.can_cherry_pick is False

    def test_package_load_without_id(self) -> None:
        """Test loading a package from dictionary without ID (backward compatibility)."""
        package_definition = {
            "title": "Test Package",
            "description": "A test package",
            "canCherryPick": True,
        }
        
        package = Package.load("test_package", package_definition)
        
        assert package.name == "test_package"
        assert package.title == "Test Package"
        assert package.description == "A test package"
        assert package.id is None
        assert package.can_cherry_pick is True

    def test_package_load_minimal(self) -> None:
        """Test loading a package with minimal required fields."""
        package_definition = {
            "title": "Minimal Package",
        }
        
        package = Package.load("minimal", package_definition)
        
        assert package.name == "minimal"
        assert package.title == "Minimal Package"
        assert package.description is None
        assert package.id is None
        assert package.can_cherry_pick is True

    def test_packages_load_with_mixed_ids(self) -> None:
        """Test loading packages where some have IDs and some don't."""
        packages_toml_content = """
[library]
title = "Test Library"

[packages.with_id]
title = "Package With ID"
description = "A package that has an ID"
id = "package-with-id-123"
modules = []

[packages.without_id]
title = "Package Without ID"
description = "A package that doesn't have an ID"
modules = []
"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            packages_file = temp_path / "packages.toml"
            packages_file.write_text(packages_toml_content)
            
            # Create empty module directories to avoid module loading errors
            (temp_path / "modules").mkdir()
            
            packages = Packages.load(temp_path)
            
            assert len(packages) == 2
            
            # Check package with ID
            with_id = packages["with_id"]
            assert with_id.title == "Package With ID"
            assert with_id.id == "package-with-id-123"
            
            # Check package without ID
            without_id = packages["without_id"]
            assert without_id.title == "Package Without ID"
            assert without_id.id is None


class TestModuleToml:
    """Test ModuleToml data class with ID field support."""

    def test_module_toml_creation_with_id(self) -> None:
        """Test creating a ModuleToml with an ID."""
        module_toml = ModuleToml(
            title="Test Module",
            id="test-module-456",
            is_selected_by_default=True,
        )
        
        assert module_toml.title == "Test Module"
        assert module_toml.id == "test-module-456"
        assert module_toml.is_selected_by_default is True
        assert len(module_toml.dependencies) == 0

    def test_module_toml_creation_without_id(self) -> None:
        """Test creating a ModuleToml without an ID (backward compatibility)."""
        module_toml = ModuleToml(
            title="Test Module",
            is_selected_by_default=False,
        )
        
        assert module_toml.title == "Test Module"
        assert module_toml.id is None
        assert module_toml.is_selected_by_default is False

    def test_module_toml_load_with_id(self) -> None:
        """Test loading ModuleToml from dictionary with ID."""
        data = {
            "module": {
                "title": "Test Module",
                "id": "test-module-456",
                "is_selected_by_default": True,
            },
            "dependencies": {
                "modules": ["other_module"],
            },
        }
        
        module_toml = ModuleToml.load(data)
        
        assert module_toml.title == "Test Module"
        assert module_toml.id == "test-module-456"
        assert module_toml.is_selected_by_default is True
        assert "other_module" in module_toml.dependencies

    def test_module_toml_load_without_id(self) -> None:
        """Test loading ModuleToml from dictionary without ID (backward compatibility)."""
        data = {
            "module": {
                "title": "Test Module",
                "is_selected_by_default": False,
            },
        }
        
        module_toml = ModuleToml.load(data)
        
        assert module_toml.title == "Test Module"
        assert module_toml.id is None
        assert module_toml.is_selected_by_default is False

    def test_module_toml_load_minimal(self) -> None:
        """Test loading ModuleToml with minimal data."""
        data = {}
        
        module_toml = ModuleToml.load(data)
        
        assert module_toml.title is None
        assert module_toml.id is None
        assert module_toml.is_selected_by_default is False
        assert len(module_toml.dependencies) == 0

    def test_module_toml_load_from_file_with_id(self) -> None:
        """Test loading ModuleToml from a TOML file with ID."""
        toml_content = """
[module]
title = "Test Module"
id = "test-module-456"
is_selected_by_default = true

[dependencies]
modules = ["dependency1", "dependency2"]
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_content)
            temp_path = Path(f.name)
        
        try:
            module_toml = ModuleToml.load(temp_path)
            
            assert module_toml.title == "Test Module"
            assert module_toml.id == "test-module-456"
            assert module_toml.is_selected_by_default is True
            assert len(module_toml.dependencies) == 2
            assert "dependency1" in module_toml.dependencies
            assert "dependency2" in module_toml.dependencies
        finally:
            temp_path.unlink()

    def test_module_toml_load_from_file_without_id(self) -> None:
        """Test loading ModuleToml from a TOML file without ID (backward compatibility)."""
        toml_content = """
[module]
title = "Test Module"
is_selected_by_default = false
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_content)
            temp_path = Path(f.name)
        
        try:
            module_toml = ModuleToml.load(temp_path)
            
            assert module_toml.title == "Test Module"
            assert module_toml.id is None
            assert module_toml.is_selected_by_default is False
        finally:
            temp_path.unlink()
