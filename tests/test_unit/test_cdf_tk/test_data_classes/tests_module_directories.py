from pathlib import Path

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories, ModuleLocation
from tests import data


class TestModuleDirectories:
    def test_load(self):
        cognite_modules = data.PROJECT_FOR_TEST / COGNITE_MODULES
        expected = ModuleDirectories(
            [
                ModuleLocation(
                    relative_path=Path("a_module"),
                    root_module="cognite_modules",
                    root_absolute_path=cognite_modules,
                ),
                ModuleLocation(
                    relative_path=Path("another_module"),
                    root_module="cognite_modules",
                    root_absolute_path=cognite_modules,
                ),
                ModuleLocation(
                    relative_path=Path("parent_module") / "child_module",
                    root_module="cognite_modules",
                    root_absolute_path=cognite_modules,
                ),
            ]
        )
        actual = ModuleDirectories.load(data.PROJECT_FOR_TEST)

        assert actual == expected
