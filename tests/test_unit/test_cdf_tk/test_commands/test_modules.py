from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import Packages


@pytest.fixture(scope="session")
def selected_packages() -> Packages:
    return Packages.load(BUILTIN_MODULES_PATH)


class TestModulesCommand:
    def test_modules_command(self, selected_packages: Packages, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(organization_dir=target_path, selected_packages=selected_packages, environments=["dev"], mode=None)

        assert Path(target_path).exists()
        assert Path(target_path / "modules" / "infield" / "cdf_infield_common").exists()

    def test_modules_command_with_env(self, selected_packages: Packages, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(
            organization_dir=target_path, selected_packages=selected_packages, environments=["dev", "prod"], mode=None
        )

        assert Path(target_path / "config.dev.yaml").exists()
        assert Path(target_path / "config.prod.yaml").exists()

    def test_config(self, selected_packages: Packages, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(organization_dir=target_path, selected_packages=selected_packages, environments=["dev"], mode=None)

        config = yaml.safe_load(Path(target_path / "config.dev.yaml").read_text())
        assert config["variables"]["modules"]["infield"]["first_location"] == "oid"

    def test_adding(self, selected_packages: Packages, tmp_path: Path) -> None:
        target_path = tmp_path / "repo_root"
        cmd = ModulesCommand(print_warning=True, skip_tracking=True)

        first_batch = Packages({"infield": selected_packages["infield"]})
        second_batch = Packages({"quickstart": selected_packages["inrobot"]})

        cmd._create(organization_dir=target_path, selected_packages=first_batch, environments=["qa"], mode=None)
        cmd._create(organization_dir=target_path, selected_packages=second_batch, environments=["qa"], mode="update")

        config = yaml.safe_load(Path(target_path / "config.qa.yaml").read_text())
        assert config["variables"]["modules"]["infield"]["first_location"] is not None
        assert (target_path / "modules" / "infield" / "cdf_infield_common").is_dir()

        assert config["variables"]["modules"]["inrobot"]["first_location"] is not None
        assert (target_path / "modules" / "inrobot" / "cdf_inrobot_common").is_dir()
