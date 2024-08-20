from pathlib import Path

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES
from cognite_toolkit._cdf_tk.data_classes import Environment, ModuleDirectories, ModuleLocation
from tests import data


class TestModuleDirectories:
    def test_load(self) -> None:
        cognite_modules = Path(COGNITE_MODULES)
        expected = ModuleDirectories(
            [
                ModuleLocation(
                    relative_path=cognite_modules / Path("a_module"),
                    source_absolute_path=data.PROJECT_FOR_TEST,
                ),
                ModuleLocation(
                    relative_path=cognite_modules / Path("another_module"),
                    source_absolute_path=data.PROJECT_FOR_TEST,
                ),
                ModuleLocation(
                    relative_path=cognite_modules / Path("parent_module") / "child_module",
                    source_absolute_path=data.PROJECT_FOR_TEST,
                ),
            ]
        )
        actual = ModuleDirectories.load(
            data.PROJECT_FOR_TEST, Environment("dev", "anders", "dev", selected=[f"{COGNITE_MODULES}/"]), {}
        )

        assert actual == expected
