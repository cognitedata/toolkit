from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.resource_ios import TransformationIO, ViewIO
from cognite_toolkit._cdf_tk.rules._transformations import TransformationRuleSet, select_aliases

QUERY = "-- from comment\nSELECT\n  p.externalId AS externalId,\n  node_reference('a', b.externalId) AS rel,\n  b.dob AS dateOfBirth\nFROM x AS p"


def built_resource(tmp_path: Path, crud_cls: Any, identifier: Identifier, content: dict[str, Any]) -> BuiltResource:
    source_path = (tmp_path / f"{crud_cls.kind}.yaml").resolve()
    return BuiltResource(
        identifier=identifier,
        source_hash="hash",
        type=crud_cls.as_crud_type(),
        source_path=source_path,
        build_path=source_path,
        built_content=content,
        crud_cls=crud_cls,
        has_syntax_error=False,
        module_id=ModuleId(id=Path("modules/my"), path=tmp_path),
    )


def run_rule(tmp_path: Path, view_properties: list[str]) -> list[str]:
    view = built_resource(
        tmp_path,
        ViewIO,
        ViewId(space="sp", external_id="Person", version="v1"),
        {
            "space": "sp",
            "externalId": "Person",
            "version": "v1",
            "properties": {
                name: {
                    "container": {"space": "sp", "externalId": "Person", "type": "container"},
                    "containerPropertyIdentifier": name,
                }
                for name in view_properties
            },
        },
    )
    transformation = built_resource(
        tmp_path,
        TransformationIO,
        ExternalId(external_id="t"),
        {
            "externalId": "t",
            "name": "t",
            "ignoreNullFields": True,
            "destination": {"type": "nodes", "view": {"space": "sp", "externalId": "Person", "version": "v1"}},
            "query": QUERY,
        },
    )
    module = BuiltModule(module_id=view.module_id, resources=[view, transformation], yaml_line_count=0)
    return [insight.message for insight in TransformationRuleSet([module]).validate()]


def test_select_aliases_ignores_comments_and_nested_commas() -> None:
    assert select_aliases(QUERY) == ["externalId", "rel", "dateOfBirth"]


def test_warns_about_selected_column_without_view_property(tmp_path: Path) -> None:
    assert run_rule(tmp_path, ["rel", "dateOfBirth"]) == []

    messages = run_rule(tmp_path, ["rel"])
    assert len(messages) == 1
    assert "dateOfBirth" in messages[0]
