from pathlib import Path

import pytest
import typer
import yaml
from _pytest.monkeypatch import MonkeyPatch
from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands import ResourcesCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import SpaceCRUD
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource
from tests.test_unit.utils import MockQuestionary


class _StubResource(ToolkitResource):
    external_id: str = Field(description="The external ID.")
    name: str = Field(description="The name.")
    runtime: str = Field(default="py311", description="Runtime version.")
    metadata: dict[str, str] | None = Field(default=None, description="Metadata.")
    description: str | None = Field(default=None, description="Optional description.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class _StubCRUD:
    yaml_cls = _StubResource

    @classmethod
    def doc_url(cls) -> str:
        return "https://example.com/api"


class TestResourcesCreateCommand:
    def test_create_success(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method with multiple kinds."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_modeling").mkdir(parents=True, exist_ok=True)

        cmd.create(
            organization_dir=organization_dir,
            module_name="test_module",
            kind=["Space", "Container"],
            prefix="my",
            verbose=False,
        )
        assert (modules_dir / "data_modeling" / "my.Space.yaml").exists()
        assert (modules_dir / "data_modeling" / "containers" / "my.Container.yaml").exists()

    def test_create_fuzzy_match_fails_gracefully(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test create method with fuzzy matching for kind suggests and exits."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_modeling").mkdir(parents=True, exist_ok=True)

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
        (modules_dir / "data_modeling").mkdir(parents=True, exist_ok=True)

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
        assert (modules_dir / "data_modeling" / "my_Space.Space.yaml").exists()

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
        (modules_dir / "data_modeling").mkdir(parents=True, exist_ok=True)

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [modules_dir]):
            cmd.create(
                organization_dir=organization_dir,
                module_name=None,
                kind=["Space"],
                verbose=False,
            )
        assert (modules_dir / "data_modeling" / "my_Space.Space.yaml").exists()

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
        assert (new_module_path / "data_modeling" / "my_Space.Space.yaml").exists()

    def test_create_interactive_kind_selection(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test interactive kind selection when kind is not provided."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_modeling").mkdir(parents=True, exist_ok=True)

        def return_space_crud(*args):
            return SpaceCRUD

        with MockQuestionary(ResourcesCommand.__module__, monkeypatch, [return_space_crud]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                kind=None,
                verbose=False,
            )
        assert (modules_dir / "data_modeling" / "my_Space.Space.yaml").exists()

    def test_yaml_content_ordering_and_comments(self) -> None:
        """Required fields first, then optional with value, then optional null. Each field has an inline comment."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        content = cmd._get_resource_yaml_content(_StubCRUD)  # type: ignore[arg-type]

        lines = content.splitlines()
        yaml_lines = [line for line in lines if not line.startswith("#") and line.strip()]
        parsed = yaml.safe_load("\n".join(line.split("  #")[0] for line in yaml_lines))

        # Required fields use placeholder strings
        assert parsed["externalId"] == "<externalId>"
        assert parsed["name"] == "<name>"

        # Optional field with non-null default
        assert parsed["runtime"] == "py311"

        # Optional null field
        assert parsed["description"] is None

        # Every field's first YAML line has an inline comment
        first_lines = [line for line in lines if not line.startswith("#") and line.strip() and not line.startswith(" ")]
        for line in first_lines:
            assert "  #" in line, f"Missing comment on first line: {line!r}"

        # Multi-line values (e.g. dict) have the comment only on the first line
        metadata_lines = [line for line in lines if "metadata" in line]
        assert any("  #" in line for line in metadata_lines), "metadata key line should have a comment"
        continuation_lines = [line for line in metadata_lines if line.startswith(" ")]
        assert all("  #" not in line for line in continuation_lines), "continuation lines should not have comments"

        # Ordering: required → optional with value → optional null
        field_names = [line.split(":")[0].strip() for line in first_lines]
        assert field_names.index("externalId") < field_names.index("runtime") < field_names.index("description")

    def test_create_interactive_kind_selection_abort(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test interactive kind selection when kind is not provided and user aborts."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        modules_dir = organization_dir / MODULES / "test_module"
        modules_dir.mkdir(parents=True, exist_ok=True)
        (modules_dir / "data_modeling").mkdir(parents=True, exist_ok=True)

        with pytest.raises(typer.Exit), MockQuestionary(ResourcesCommand.__module__, monkeypatch, [None]):
            cmd.create(
                organization_dir=organization_dir,
                module_name="test_module",
                kind=None,
                verbose=False,
            )
