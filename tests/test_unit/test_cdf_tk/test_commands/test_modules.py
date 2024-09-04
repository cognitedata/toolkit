from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.data_classes._packages import Packages, SelectableModule
from tests.data import PACKAGE_FOR_TEST


@pytest.fixture(scope="session")
def selected_packages() -> dict[str, list[SelectableModule]]:
    available = Packages.load(PACKAGE_FOR_TEST)[0]
    return {available.name: available.modules}


class TestModulesCommand:
    def test_modules_command(self, selected_packages: dict[str, list[SelectableModule]], tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "_"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(init_dir=target_path, selected=selected_packages, environments=[], mode=None)

        assert Path(target_path).exists()
        assert Path(target_path / "modules" / "infield" / "cdf_infield_common").exists()

    def test_modules_command_with_env(
        self, selected_packages: dict[str, list[SelectableModule]], tmp_path: Path
    ) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "_"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(init_dir=target_path, selected=selected_packages, environments=["dev", "prod"], mode=None)

        assert Path(target_path / "config.dev.yaml").exists()
        assert Path(target_path / "config.prod.yaml").exists()
        assert Path(target_path.parent / "cdf.toml").exists()
