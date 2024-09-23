from __future__ import annotations

from pathlib import Path

from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories, ModuleLocation
from tests import data


class TestModuleDirectories:
    def test_load(self) -> None:
        cognite_modules = Path(BUILTIN_MODULES)
        expected = ModuleDirectories(
            [
                ModuleLocation(
                    dir=cognite_modules / Path("a_module"),
                    source_absolute_path=data.PROJECT_FOR_TEST,
                    is_selected=True,
                    source_paths=[],
                ),
                ModuleLocation(
                    dir=cognite_modules / Path("another_module"),
                    source_absolute_path=data.PROJECT_FOR_TEST,
                    is_selected=True,
                    source_paths=[],
                ),
                ModuleLocation(
                    dir=cognite_modules / Path("parent_module") / "child_module",
                    source_absolute_path=data.PROJECT_FOR_TEST,
                    is_selected=True,
                    source_paths=[],
                ),
            ]
        )
        actual = ModuleDirectories.load(data.PROJECT_FOR_TEST, {("modules",)})

        assert actual == expected
