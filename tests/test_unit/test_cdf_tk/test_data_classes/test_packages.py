from __future__ import annotations

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

    def test_package_with_id(self) -> None:
        """Test Package creation and loading with ID field."""
        # Test creation with ID
        package = Package(name="test", title="Test Package", id="test-123")
        assert package.id == "test-123"

        # Test loading with ID
        package_data = {"title": "Test Package", "id": "test-456"}
        loaded_package = Package.load("test", package_data)
        assert loaded_package.id == "test-456"

        # Test loading without ID
        package_data_no_id = {"title": "Test Package"}
        loaded_package_no_id = Package.load("test", package_data_no_id)
        assert loaded_package_no_id.id is None

    def test_module_toml_with_id(self) -> None:
        """Test ModuleToml creation and loading with ID field."""
        # Test creation with ID
        module_toml = ModuleToml(title="Test Module", id="module-123")
        assert module_toml.id == "module-123"

        # Test loading with ID
        data_with_id = {"module": {"title": "Test Module", "id": "module-456"}}
        loaded_toml = ModuleToml.load(data_with_id)
        assert loaded_toml.id == "module-456"

        # Test loading without ID
        data_no_id = {"module": {"title": "Test Module"}}
        loaded_toml_no_id = ModuleToml.load(data_no_id)
        assert loaded_toml_no_id.id is None
