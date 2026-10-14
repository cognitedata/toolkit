"""Tests for the sandbox session."""

import shutil
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.sandbox import AddResource, ModelingSession, RemovePath, SetExtra, SetPath
from cognite_toolkit._cdf_tk.sandbox._render import render_source_file
from cognite_toolkit._cdf_tk.sandbox._workspace import Workspace
from tests.constants import REPO_ROOT

COMPLETE_ORG = REPO_ROOT / "tests/data/complete_org"
MODULE = "modules/my_example_module"

NAME_CONTAINER = {"space": "sp_schema", "externalId": "NameContainer"}
PARENT_VIEW = {"space": "sp_schema", "externalId": "ParentView", "version": "v1"}
TEXT_PROPERTY = {"type": {"type": "text"}}


def load_session(org_dir: Path, module: str = MODULE) -> ModelingSession:
    workspace = Workspace()
    workspace.load(org_dir, org_dir / "config.dev.yaml", [module])
    return workspace.get_session()


@pytest.fixture
def org_dir(tmp_path: Path) -> Path:
    dest = tmp_path / "complete_org"
    shutil.copytree(COMPLETE_ORG, dest)
    return dest


@pytest.fixture
def dm_dir(org_dir: Path) -> Path:
    return org_dir / MODULE / "data_modeling"


def container_op(path: list[str | int], value: object) -> SetPath:
    return SetPath(kind="Container", identifier=NAME_CONTAINER, path=path, value=value)


def test_unedited_data_modeling_files_round_trip_byte_identical(org_dir: Path, dm_dir: Path) -> None:
    session = load_session(org_dir)
    data_modeling_files = [source for source in session.files if source.path.parent == dm_dir.resolve()]
    assert data_modeling_files
    for source in data_modeling_files:
        assert render_source_file(source) == source.original_text, source.path.name


def test_loads_kinds_beyond_data_modeling(org_dir: Path) -> None:
    session = load_session(org_dir)
    kinds = {source.read.source_type.kind for source in session.files}
    assert {"Container", "Transformation", "Workflow", "Function"} <= kinds


def test_set_path_adds_property_without_touching_other_lines(org_dir: Path, dm_dir: Path) -> None:
    session = load_session(org_dir)
    original = (dm_dir / "name.Container.yaml").read_text()

    result = session.apply(container_op(["properties", "tag"], TEXT_PROPERTY))
    assert result.ok, result.insights
    assert session.write() == [(dm_dir / "name.Container.yaml").resolve()]

    updated = (dm_dir / "name.Container.yaml").read_text()
    assert updated.startswith(original)
    assert "  tag:\n    type:\n      type: text\n" in updated


def test_remove_path_removes_property(org_dir: Path, dm_dir: Path) -> None:
    session = load_session(org_dir)
    path = dm_dir / "name.Container.yaml"

    result = session.apply(RemovePath(kind="Container", identifier=NAME_CONTAINER, path=["properties", "direct"]))
    assert result.ok, result.insights
    assert "-  direct:" in session.diff()[path.resolve()]
    session.write()
    updated = path.read_text()
    assert "direct:" not in updated
    assert "name:" in updated


def test_invalid_value_is_rejected_and_rolled_back(org_dir: Path, dm_dir: Path) -> None:
    session = load_session(org_dir)
    original = (dm_dir / "name.Container.yaml").read_text()

    result = session.apply(container_op(["description"], {"not": "text"}))
    assert not result.ok
    assert session.diff() == {}
    assert session.write() == []
    assert (dm_dir / "name.Container.yaml").read_text() == original

    # The session is still usable after a rejected operation.
    assert session.apply(container_op(["properties", "tag"], TEXT_PROPERTY)).ok


def test_unknown_path_raises_and_rolls_back(org_dir: Path) -> None:
    session = load_session(org_dir)
    with pytest.raises(ValueError, match="does not exist"):
        session.apply(container_op(["missing", "tag"], TEXT_PROPERTY))
    assert session.diff() == {}


def test_set_path_appends_to_list_and_adds_view_property(org_dir: Path, dm_dir: Path) -> None:
    session = load_session(org_dir)
    view_property = {
        "container": {"space": "sp_schema", "externalId": "NameContainer", "type": "container"},
        "containerPropertyIdentifier": "direct",
    }
    assert session.apply(
        SetPath(kind="View", identifier=PARENT_VIEW, path=["properties", "directAgain"], value=view_property)
    ).ok
    assert session.apply(
        SetPath(
            kind="DataModel",
            identifier={"space": "DataModelSpace", "externalId": "DataModelLocation", "version": "1_0_0"},
            path=["views", "-"],
            value={"space": "sp_schema", "externalId": "ParentView", "version": "v1", "type": "view"},
        )
    ).ok
    session.write()

    assert "containerPropertyIdentifier: direct\n" in (dm_dir / "parent.View.yaml").read_text()
    model_text = (dm_dir / "location.DataModel.yaml").read_text()
    # The existing view stays first, the new one is appended.
    assert model_text.index("CogniteFile") < model_text.index("ParentView")


def test_add_resource_creates_file_under_sub_folder(org_dir: Path, dm_dir: Path) -> None:
    session = load_session(org_dir)
    result = session.apply(
        AddResource(
            kind="Container",
            resource={"space": "sp_schema", "externalId": "NewThing", "properties": {"name": TEXT_PROPERTY}},
        )
    )
    assert result.ok, result.insights
    expected = (dm_dir / "containers" / "NewThing.Container.yaml").resolve()
    assert session.write() == [expected]
    assert "externalId: NewThing" in expected.read_text()
    assert session.find("Container", {"space": "sp_schema", "externalId": "NewThing"})


def test_add_resource_rejects_existing_identifier(org_dir: Path) -> None:
    session = load_session(org_dir)
    with pytest.raises(ValueError, match="already exists"):
        session.apply(AddResource(kind="Container", resource={**NAME_CONTAINER, "properties": {}}))


def test_set_extra_replaces_query_and_keeps_variable_tokens(org_dir: Path) -> None:
    session = load_session(org_dir)
    sql_path = org_dir / MODULE / "transformations" / "first.transformation.sql"
    identifier = {"externalId": "tr_first_transformation"}
    query = "select 'a' as externalId from `db_{{ example_variable }}`.`table`"

    assert session.apply(SetExtra(kind="Transformation", identifier=identifier, field="query", text=query)).ok
    assert session.write() == [sql_path.resolve()]
    assert sql_path.read_text() == query


def test_set_extra_is_checked_by_rules_straight_away(org_dir: Path) -> None:
    session = load_session(org_dir)
    identifier = {"externalId": "tr_first_transformation"}
    destination = {"type": "nodes", "view": {"space": "sp_schema", "externalId": "ParentView", "version": "v1"}}
    assert session.apply(
        SetPath(kind="Transformation", identifier=identifier, path=["destination"], value=destination)
    ).ok

    result = session.apply(
        SetExtra(kind="Transformation", identifier=identifier, field="query", text="select 1 as nope from t")
    )
    assert any("selects 'nope'" in insight.message for insight in result.insights)


def test_edit_preserves_comments_and_variables(tmp_path: Path) -> None:
    org = tmp_path / "org"
    module = org / "modules" / "mod" / "data_modeling"
    module.mkdir(parents=True)
    (org / "config.dev.yaml").write_text(
        "environment:\n  name: dev\n  project: p\n  type: dev\n  selected:\n    - modules/\n"
        "variables:\n  modules:\n    mod:\n      prefix: sp\n"
    )
    original = (
        "# Pump container\n"
        "space: '{{ prefix }}_schema'\n"
        "externalId: Pump\n"
        "usedFor: node\n"
        "properties:\n"
        "  name:\n"
        "    type:\n"
        "      type: text\n"
        "    nullable: true  # keep this comment\n"
    )
    (module / "pump.Container.yaml").write_text(original)
    session = load_session(org, "modules/mod")

    identifier = {"space": "sp_schema", "externalId": "Pump"}
    result = session.apply(
        SetPath(kind="Container", identifier=identifier, path=["properties", "tag"], value=TEXT_PROPERTY)
    )
    assert result.ok, result.insights
    session.write()

    text = (module / "pump.Container.yaml").read_text()
    assert text.startswith(original)
    assert "{{ prefix }}_schema" in text
    assert "tag:" in text
