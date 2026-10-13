"""Tests for the data modeling sandbox POC."""

import shutil
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    TextProperty,
    ViewCorePropertyRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError
from cognite_toolkit._cdf_tk.sandbox import (
    AddContainer,
    AddContainerProperty,
    AddView,
    AddViewProperty,
    AddViewToDataModel,
    ModelingSession,
    RemoveContainerProperty,
    RemoveViewProperty,
)
from cognite_toolkit._cdf_tk.sandbox._render import render_source_file
from tests.constants import REPO_ROOT

COMPLETE_ORG_DM = REPO_ROOT / "tests/data/complete_org/modules/my_example_module/data_modeling"


@pytest.fixture
def dm_root(tmp_path: Path) -> Path:
    dest = tmp_path / "data_modeling"
    shutil.copytree(COMPLETE_ORG_DM, dest)
    return dest


def test_noop_round_trip_is_byte_identical(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    assert session.requests
    for source in session.files:
        rendered = render_source_file(source, session._requests_by_index(source))
        assert rendered == source.original_text, source.path.name


def test_add_container_property_diff_is_only_new_property(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    container_id = ContainerId(space="sp_schema", external_id="NameContainer")
    original = (dm_root / "name.Container.yaml").read_text()

    result = session.apply(
        AddContainerProperty(
            container=container_id,
            name="tag",
            definition=ContainerPropertyDefinition(type=TextProperty(type="text")),
        )
    )
    assert result.ok, result.insights
    written = session.write()
    assert written == [dm_root / "name.Container.yaml"]

    updated = (dm_root / "name.Container.yaml").read_text()
    assert "tag:" in updated
    assert "type: text" in updated
    # Existing content stays; only the new property is added.
    assert "name:" in updated
    assert "direct:" in updated
    assert updated.count("externalId: NameContainer") == 1
    # Diff should not rewrite unrelated keys.
    assert "usedFor: node" in updated
    assert original.split("properties:")[0] == updated.split("properties:")[0]


def test_remove_container_property_updates_yaml(dm_root: Path) -> None:
    container_id = ContainerId(space="sp_schema", external_id="NameContainer")
    path = dm_root / "name.Container.yaml"

    session = ModelingSession.load(dm_root)
    assert session.apply(
        AddContainerProperty(
            container=container_id,
            name="tag",
            definition=ContainerPropertyDefinition(type=TextProperty(type="text")),
        )
    ).ok
    session.write()
    assert "tag:" in path.read_text()

    session = ModelingSession.load(dm_root)
    result = session.apply(RemoveContainerProperty(container=container_id, name="tag"))
    assert result.ok, result.insights
    diff = session.diff()[path]
    assert "-  tag:" in diff or "- tag:" in diff
    session.write()
    assert "tag:" not in path.read_text()
    assert "name:" in path.read_text()
    assert "direct:" in path.read_text()


def test_remove_container_property_rejects_when_view_still_maps_to_it(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    container_id = ContainerId(space="sp_schema", external_id="NameContainer")
    original = (dm_root / "name.Container.yaml").read_text()

    result = session.apply(RemoveContainerProperty(container=container_id, name="name"))
    assert not result.ok
    assert any(isinstance(insight, BuildError) for insight in result.insights)
    session.write()
    assert (dm_root / "name.Container.yaml").read_text() == original


def test_remove_view_property_updates_yaml(dm_root: Path) -> None:
    view_id = ViewId(space="sp_schema", external_id="ParentView", version="v1")
    path = dm_root / "parent.View.yaml"

    session = ModelingSession.load(dm_root)
    assert session.apply(
        AddViewProperty(
            view=view_id,
            name="directAgain",
            definition=ViewCorePropertyRequest(
                container=ContainerId(space="sp_schema", external_id="NameContainer"),
                container_property_identifier="direct",
            ),
        )
    ).ok
    session.write()
    assert "directAgain:" in path.read_text()

    session = ModelingSession.load(dm_root)
    result = session.apply(RemoveViewProperty(view=view_id, name="directAgain"))
    assert result.ok, result.insights
    session.write()
    assert "directAgain:" not in path.read_text()
    assert "name:" in path.read_text()


def test_add_container_property_preserves_comments_and_variables(tmp_path: Path) -> None:
    root = tmp_path / "data_modeling"
    root.mkdir()
    source = root / "pump.Container.yaml"
    original = """\
# Pump container
space: '{{sp_id_prefix}}_{{ent_prefix}}'
externalId: Pump
usedFor: node
properties:
  name:
    type:
      type: text
    nullable: true  # keep this comment
"""
    source.write_text(original)
    variables = [
        BuildVariable(id=Path("modules/sp_id_prefix"), value="sp", is_selected=True),
        BuildVariable(id=Path("modules/ent_prefix"), value="ent", is_selected=True),
    ]
    session = ModelingSession.load(root, variables=variables)
    result = session.apply(
        AddContainerProperty(
            container=ContainerId(space="sp_ent", external_id="Pump"),
            name="tag",
            definition=ContainerPropertyDefinition(type=TextProperty(type="text"), nullable=False),
        )
    )
    assert result.ok, result.insights
    session.write()
    text = source.read_text()
    # Existing authored lines must stay byte-identical; only the new property is appended.
    assert text.startswith(original.rstrip("\n") + "\n")
    assert "tag:" in text
    assert "{{sp_id_prefix}}" in text
    assert "{{ent_prefix}}" in text
    assert "# keep this comment" in text


def test_add_container_creates_file_under_containers(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    result = session.apply(
        AddContainer(
            container=ContainerRequest(
                space="sp_schema",
                external_id="NewThing",
                properties={"name": ContainerPropertyDefinition(type=TextProperty(type="text"))},
            )
        )
    )
    assert result.ok, result.insights
    written = session.write()
    expected = dm_root / "containers" / "NewThing.Container.yaml"
    assert expected in written
    assert expected.is_file()
    assert "externalId: NewThing" in expected.read_text()


def test_add_view_property_rejects_unknown_local_container_property(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    view_id = ViewId(space="sp_schema", external_id="ParentView", version="v1")
    before = session.get_view(view_id).model_copy(deep=True)
    original_text = (dm_root / "parent.View.yaml").read_text()

    result = session.apply(
        AddViewProperty(
            view=view_id,
            name="missingProp",
            definition=ViewCorePropertyRequest(
                container=ContainerId(space="sp_schema", external_id="NameContainer"),
                container_property_identifier="doesNotExist",
            ),
        )
    )
    assert not result.ok
    assert any(isinstance(insight, BuildError) for insight in result.insights)
    assert session.get_view(view_id).dump() == before.dump()
    assert not any(source.dirty for source in session.files)
    session.write()
    assert (dm_root / "parent.View.yaml").read_text() == original_text


def test_add_view_property_accepts_existing_container_property(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    view_id = ViewId(space="sp_schema", external_id="ParentView", version="v1")
    result = session.apply(
        AddViewProperty(
            view=view_id,
            name="directAgain",
            definition=ViewCorePropertyRequest(
                container=ContainerId(space="sp_schema", external_id="NameContainer"),
                container_property_identifier="direct",
            ),
        )
    )
    assert result.ok, result.insights
    session.write()
    text = (dm_root / "parent.View.yaml").read_text()
    assert "directAgain:" in text
    assert "containerPropertyIdentifier: direct" in text


def test_add_view_creates_file_under_views(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    result = session.apply(
        AddView(
            view=ViewRequest(
                space="sp_schema",
                external_id="BrandNewView",
                version="v1",
                properties={
                    "name": ViewCorePropertyRequest(
                        container=ContainerId(space="sp_schema", external_id="NameContainer"),
                        container_property_identifier="name",
                    )
                },
            )
        )
    )
    assert result.ok, result.insights
    written = session.write()
    expected = dm_root / "views" / "BrandNewView.View.yaml"
    assert expected in written
    assert expected.is_file()


def test_add_view_to_data_model_appends_without_reordering(dm_root: Path) -> None:
    session = ModelingSession.load(dm_root)
    data_model_id = DataModelId(space="DataModelSpace", external_id="DataModelLocation", version="1_0_0")
    # Add a local view first so AddViewToDataModel's existence check passes.
    new_view_id = ViewId(space="sp_schema", external_id="LocalForModel", version="v1")
    assert session.apply(
        AddView(
            view=ViewRequest(
                space=new_view_id.space,
                external_id=new_view_id.external_id,
                version=new_view_id.version,
                properties={
                    "name": ViewCorePropertyRequest(
                        container=ContainerId(space="sp_schema", external_id="NameContainer"),
                        container_property_identifier="name",
                    )
                },
            )
        )
    ).ok

    result = session.apply(AddViewToDataModel(data_model=data_model_id, view=new_view_id))
    assert result.ok, result.insights
    session.write()

    text = (dm_root / "location.DataModel.yaml").read_text()
    # Original CogniteFile entry stays first; new view is appended.
    cognite_pos = text.index("CogniteFile")
    local_pos = text.index("LocalForModel")
    assert cognite_pos < local_pos
    assert text.count("externalId:") >= 3
