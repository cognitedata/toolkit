from __future__ import annotations

from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.data_classes._packages import Package


class TestPackages:
    def test_package_creation_with_id(self) -> None:
        package = Package(name="test", title="Test Package", id="test-123")
        assert package.id == "test-123"

    def test_package_load_with_id(self) -> None:
        package_data = {"title": "Test Package", "id": "test-456"}
        loaded_package = Package.load("test", package_data)
        assert loaded_package.id == "test-456"

    def test_package_load_without_id(self) -> None:
        package_data = {"title": "Test Package"}
        loaded_package = Package.load("test", package_data)
        assert loaded_package.id is None

    def test_module_toml_creation_with_id(self) -> None:
        module_toml = ModuleToml(title="Test Module", id="module-123")
        assert module_toml.id == "module-123"

    def test_module_toml_load_with_id(self) -> None:
        data = {"module": {"title": "Test Module", "id": "module-456"}}
        loaded_toml = ModuleToml.load(data)
        assert loaded_toml.id == "module-456"

    def test_module_toml_load_without_id(self) -> None:
        data = {"module": {"title": "Test Module"}}
        loaded_toml = ModuleToml.load(data)
        assert loaded_toml.id is None
