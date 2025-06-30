from __future__ import annotations

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
