from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from pytest import raises

from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.data_classes._packages import Packages


class TestPackages:
    def test_load(self) -> None:
        packages = Packages.load(BUILTIN_MODULES_PATH)
        assert packages is not None
        assert len(packages) >= 5
        assert "infield" in packages
        infield = packages["infield"]
        assert len(infield.modules) > 0
        assert "cdf_infield_common" in infield.module_names
        assert "cdf_infield_location" in infield.module_names

    def test_fail_on_invalid_tag(self) -> None:
        def mock_toml_load(path: Path) -> ModuleToml:
            return ModuleToml("description", frozenset(["invalid_tag", "tag2"]))

        with patch("cognite_toolkit._cdf_tk.data_classes._module_toml.ModuleToml.load", side_effect=mock_toml_load):
            # Test the overridden behavior
            with raises(ValueError):
                Packages.load(BUILTIN_MODULES_PATH)
