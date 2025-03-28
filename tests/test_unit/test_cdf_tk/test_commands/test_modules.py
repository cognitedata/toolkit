from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from questionary import Choice

from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import Package, Packages
from tests.test_unit.utils import MockQuestionary


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

    def test_adding(self, selected_packages: Packages, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        target_path = tmp_path / "repo_root"
        cmd = ModulesCommand(print_warning=True, skip_tracking=True)

        first_batch = Packages({"infield": selected_packages["infield"]})
        second_batch = Packages({"quickstart": selected_packages["inrobot"]})

        cmd._create(organization_dir=target_path, selected_packages=first_batch, environments=["qa"], mode=None)
        with monkeypatch.context() as m:
            # Mocking questionary such that questionary.confirm.ask() returns True.
            questionary_mock = MagicMock()
            # MagicMock will always return other MagicMock objects
            # which when evaluated will return True.

            m.setattr("cognite_toolkit._cdf_tk.commands.modules.questionary", questionary_mock)
            cmd._create(
                organization_dir=target_path, selected_packages=second_batch, environments=["qa"], mode="update"
            )

        config = yaml.safe_load(Path(target_path / "config.qa.yaml").read_text())
        assert config["variables"]["modules"]["infield"]["first_location"] is not None
        assert (target_path / "modules" / "infield" / "cdf_infield_common").is_dir()

        assert config["variables"]["modules"]["inrobot"]["first_location"] is not None
        assert (target_path / "modules" / "inrobot" / "cdf_inrobot_common").is_dir()

    def test_add_without_config_yaml(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        dummy_resource = "space: my_space"
        my_org = tmp_path / "my_org"
        moules = my_org / "modules"
        filepath = moules / "my_module" / "data_models" / "my.Space.yaml"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(dummy_resource)

        def select_source_system(choices: list[Choice]) -> Package:
            selected_package = next((c for c in choices if "sourcesystem" in c.title.lower()), None)
            assert selected_package is not None
            return selected_package.value

        def select_sap_events(choices: list[Choice]) -> list:
            selected_module = next(
                (c for c in choices if "sap" in c.title.lower() and "event" in c.title.lower()), None
            )
            assert selected_module is not None
            return [selected_module.value]

        answers = [select_source_system, select_sap_events, False, False]

        with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers):
            cmd.add(my_org)

        yaml_file_count = len(list(moules.rglob("*.yaml")))

        assert yaml_file_count > 1, "Expected new yaml files to b created"

        def select_sap_assets(choices: list[Choice]) -> list:
            selected_module = next(
                (c for c in choices if "sap" in c.title.lower() and "asset" in c.title.lower()), None
            )
            assert selected_module is not None
            return [selected_module.value]

        answers = [select_source_system, select_sap_assets, False, False]

        with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers):
            cmd.add(my_org)

        new_yaml_file_count = len(list(moules.rglob("*.yaml")))

        assert new_yaml_file_count > yaml_file_count, "Expected new yaml files to be created"
