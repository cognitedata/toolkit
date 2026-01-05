from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes._modules import ModuleRootDirectory, OrganizationDirMissingWarning
from tests.data import COMPLETE_ORG


class TestModules:
    def test_load_modules(self) -> None:
        modules = ModuleRootDirectory.load(COMPLETE_ORG)

        assert len(modules.modules) == 3
        assert {module.path for module in modules.modules} == {
            COMPLETE_ORG / MODULES / "my_example_module",
            COMPLETE_ORG / MODULES / "my_file_expand_module",
            COMPLETE_ORG / MODULES / "populate_model",
        }

    def test_load_selection(self) -> None:
        modules = ModuleRootDirectory.load(
            COMPLETE_ORG, selection=["my_example_module", Path(MODULES) / "my_file_expand_module"]
        )

        assert len(modules.modules) == 2
        assert {module.path for module in modules.modules} == {
            COMPLETE_ORG / MODULES / "my_example_module",
            COMPLETE_ORG / MODULES / "my_file_expand_module",
        }

    def test_warns_if_organization_dir_missing(self) -> None:
        with pytest.warns(OrganizationDirMissingWarning):
            modules = ModuleRootDirectory.load(Path("wrong"))
        assert modules.modules == []
