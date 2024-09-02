from __future__ import annotations

from unittest.mock import patch

from pytest import raises

from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.data_classes._packages import Packages
from tests.data import PACKAGE_FOR_TEST


class TestPackages:
    def test_load(self) -> None:
        packages = Packages.load(PACKAGE_FOR_TEST)
        assert packages is not None
        assert len(packages) == 3
        assert len(packages[0].modules) > 0

    def test_fail_on_invalid_tag(self) -> None:
        def mock_toml_load(path):
            return ModuleToml("1.0.0", "description", ["invalid_tag", "tag2"])

        with patch("cognite_toolkit._cdf_tk.data_classes._module_toml.ModuleToml.load", side_effect=mock_toml_load):
            # Test the overridden behavior
            with raises(ValueError):
                Packages.load(PACKAGE_FOR_TEST)
