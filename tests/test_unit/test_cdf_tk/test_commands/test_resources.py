from pathlib import Path

import pytest
import typer
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands import ResourcesCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import SpaceCRUD
from tests.test_unit.utils import MockQuestionary


class TestResourcesCreateCommand:
    def test_create_success(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method with multiple kinds."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_models").mkdir(parents=True, exist_ok=True)

        cmd.create(
            organization_dir=organization_dir,
            module_name="test_module",
            kind=["Space", "Container"],
            prefix="my",
            verbose=False,
        )
        assert (modules_dir / "data_models" / "my.Space.yaml").exists()
        assert (modules_dir / "data_models" / "containers" / "my.Container.yaml").exists()

    def test_create_fuzzy_match_fails_gracefully(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method with fuzzy matching for kind suggests and exits."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_models").mkdir(parents=True, exist_ok=True)

        with pytest.raises(typer.Exit):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                kind=["Spce"],
                verbose=False,
            )

    def test_create_unknown_kind_no_match(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method with unknown kind and no close match."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_models").mkdir(parents=True, exist_ok=True)

        with pytest.raises(typer.Exit):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                kind=["RocketShip"],
                verbose=False,
            )

    def test_create_module_not_found_create_new(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method when module doesn't exist and user creates new."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [True]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="new_module",
                kind=["Space"],
                verbose=False,
            )

        modules_dir = organization_dir / MODULES / "new_module"
        assert (modules_dir / "data_models" / "my_Space.Space.yaml").exists()

    def test_create_module_not_found_abort(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method when module doesn't exist and user aborts."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"

        with pytest.raises(typer.Exit), MockQuestionary(ResourcesCommand.__module__, monkeypatch, [False]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="non_existent",
                kind=["Space"],
                verbose=False,
            )

    def test_create_interactive_module_selection(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test interactive module selection."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "existing_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_models").mkdir(parents=True, exist_ok=True)

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [modules_dir]):
            cmd.create(
                organization_dir=organization_dir,
                module_name=None,
                kind=["Space"],
                verbose=False,
            )
        assert (modules_dir / "data_models" / "my_Space.Space.yaml").exists()

    def test_create_interactive_new_module(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test interactive selection -> Create new module."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        organization_dir.mkdir(parents=True, exist_ok=True)

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, ["NEW", "created_module"]):
            cmd.create(
                organization_dir=organization_dir,
                module_name=None,
                kind=["Space"],
                verbose=False,
            )

        new_module_path = organization_dir / MODULES / "created_module"
        assert (new_module_path / "data_models" / "my_Space.Space.yaml").exists()

    def test_create_interactive_kind_selection(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test interactive kind selection when kind is not provided."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_models").mkdir(parents=True, exist_ok=True)

        def return_space_crud(*args):
            return SpaceCRUD

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [return_space_crud]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                kind=None,
                verbose=False,
            )
        assert (modules_dir / "data_models" / "my_Space.Space.yaml").exists()

    def test_create_interactive_kind_selection_abort(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test interactive kind selection when kind is not provided and user aborts."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_models").mkdir(parents=True, exist_ok=True)

        with pytest.raises(typer.Exit), MockQuestionary(ResourcesCommand.__module__, monkeypatch, [None]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                kind=None,
                verbose=False,
            )
