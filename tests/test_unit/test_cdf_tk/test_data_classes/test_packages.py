from __future__ import annotations

from unittest.mock import patch

from pytest import raises

from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.data_classes._packages import Packages


class TestPackages:
    def test_load(self) -> None:
        packages = Packages.load(BUILTIN_MODULES_PATH)
        assert packages is not None
        assert len(packages) == 5
        assert len(packages[0].modules) > 0
        assert any(module.name == "infield" for module in packages[0].modules)

    def test_fail_on_invalid_tag(self) -> None:
        def mock_toml_load(path):
            return ModuleToml("description", ["invalid_tag", "tag2"])

        with patch("cognite_toolkit._cdf_tk.data_classes._module_toml.ModuleToml.load", side_effect=mock_toml_load):
            # Test the overridden behavior
            with raises(ValueError):
                Packages.load(BUILTIN_MODULES_PATH)
