from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes._packages import Packages


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

    def test_load_modules_with_and_without_prefix(self, tmp_path: Path) -> None:
        """Test that modules can be specified with or without 'modules/' prefix in packages.toml"""

        # Create a simple module structure
        modules_dir = tmp_path / "modules"
        modules_dir.mkdir()

        # Create a test module
        test_module_dir = modules_dir / "test_module"
        test_module_dir.mkdir()
        (test_module_dir / "module.toml").write_text("""
[module]
title = "Test Module"
""")
        # Add an auth directory to make it a valid module
        auth_dir = test_module_dir / "auth"
        auth_dir.mkdir()
        (auth_dir / "test.yaml").write_text("# test auth file")

        # Create a nested test module
        nested_dir = modules_dir / "nested"
        nested_dir.mkdir()
        nested_module_dir = nested_dir / "nested_module"
        nested_module_dir.mkdir()
        (nested_module_dir / "module.toml").write_text("""
[module]
title = "Nested Test Module"
""")
        # Add an auth directory to make it a valid module
        nested_auth_dir = nested_module_dir / "auth"
        nested_auth_dir.mkdir()
        (nested_auth_dir / "test.yaml").write_text("# test nested auth file")

        # Create packages.toml with mixed module path formats
        packages_toml_content = """
[packages.test_without_prefix]
title = "Test Without Prefix"
description = "Package with modules specified without modules/ prefix"
modules = [
    "test_module",
    "nested/nested_module"
]

[packages.test_with_prefix]
title = "Test With Prefix"
description = "Package with modules specified with modules/ prefix"
modules = [
    "modules/test_module",
    "modules/nested/nested_module"
]

[packages.test_mixed]
title = "Test Mixed"
description = "Package with mixed module path formats"
modules = [
    "test_module",
    "modules/nested/nested_module"
]
"""

        (tmp_path / "packages.toml").write_text(packages_toml_content)

        # Load packages
        packages = Packages.load(tmp_path)

        # Verify all packages loaded correctly
        assert "test_without_prefix" in packages
        assert "test_with_prefix" in packages
        assert "test_mixed" in packages

        # Verify all packages have the same modules
        expected_module_names = {"test_module", "nested_module"}

        assert packages["test_without_prefix"].module_names == expected_module_names
        assert packages["test_with_prefix"].module_names == expected_module_names
        assert packages["test_mixed"].module_names == expected_module_names

        # Verify modules have correct relative paths
        for package in packages.values():
            for module in package.modules:
                if module.name == "test_module":
                    assert module.relative_path == Path("modules/test_module")
                elif module.name == "nested_module":
                    assert module.relative_path == Path("modules/nested/nested_module")
