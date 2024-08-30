from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleDirectories
from cognite_toolkit._cdf_tk.data_classes._packages import Packages


@pytest.fixture(scope="session")
def module_directories() -> ModuleDirectories:
    # TODO: use a test data directory instead of cognite_modules
    return ModuleDirectories.load(Path(COGNITE_MODULES_PATH), set())


class TestPackages:
    def test_load(self, module_directories: ModuleDirectories) -> None:
        packages = Packages.load(module_directories)
        assert packages is not None
        assert len(packages) == 4
        assert len(packages[0].modules) > 0

    def test_load_manifests(self, module_directories: ModuleDirectories) -> None:
        packages = Packages.load(module_directories)
        package = packages[0]
        assert len(package.modules) > 0
