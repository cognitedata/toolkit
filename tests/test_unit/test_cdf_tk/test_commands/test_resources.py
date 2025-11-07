from pathlib import Path

import pytest
import typer
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.apps import DevApp
from cognite_toolkit._cdf_tk.commands import ResourcesCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from tests.test_unit.utils import MockQuestionary


class TestResourcesCreateCommand:
    def test_create_success_with_matching_file_names(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method executes successfully with matching file names and resources."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [True]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                resource_directory="data_models",
                resources=["Space", "Container"],
                file_name=["my_space", "my_container"],
                verbose=False,
            )
            assert (modules_dir / "data_models" / "my_space.Space.yaml").exists()
            assert (modules_dir / "data_models" / "containers" / "my_container.Container.yaml").exists()

    def test_create_bad_parameter_mismatched_file_names(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test that BadParameter error is raised when number of file names doesn't match number of resources."""
        organization_dir = tmp_path / "my_org"
        organization_dir.mkdir(parents=True, exist_ok=True)
        app = DevApp()

        with (
            MockQuestionary(ResourcesCommand.__module__, monkeypatch, [True]),
            pytest.raises(typer.BadParameter, match="Number of resources must match number of file names"),
        ):
            app.create(
                module="test_module",
                resource_directory="data_models",
                resources=["space", "container"],
                file_names=["my_space"],
                verbose=False,
                organization_dir=organization_dir,
            )

    def test_create_with_empty_file_names_uses_defaults(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method with empty file_names uses default file names."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [True]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                resource_directory="data_models",
                resources=["Space"],
                file_name=None,
                verbose=False,
            )
            assert (modules_dir / "data_models" / "my_Space.Space.yaml").exists()

    def test_create_with_invalid_resource_directory(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method raises error with invalid resource directory."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)

        with (
            pytest.raises(typer.Exit),
            MockQuestionary(ResourcesCommand.__module__, monkeypatch, [True]),
        ):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                resource_directory="invalid_directory",
                resources=["Space"],
                file_name=None,
                verbose=False,
            )

    def test_create_module_not_found_user_rejects(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test create method when module doesn't exist and user rejects creation."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        organization_dir.mkdir(parents=True, exist_ok=True)

        with (
            pytest.raises(typer.Exit),
            MockQuestionary(ResourcesCommand.__module__, monkeypatch, [False]),
        ):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module_not_found",
                resource_directory="data_models",
                resources=["Space"],
                file_name=None,
                verbose=False,
            )
