from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerResponse,
    DataModelResponse,
    TextProperty,
    ViewCorePropertyResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view_property import ConstraintOrIndexState
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId, ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath, RelativeDirPath
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, DataModelIO, ResourceIO, ViewIO
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet

CONTAINER_ID = ContainerId(space="my_space", external_id="MyContainer")
VIEW_ID = ViewId(space="my_space", external_id="MyView", version="v1")
DATA_MODEL_ID = DataModelId(space="my_space", external_id="MyModel", version="v1")

CONTAINER_YAML = """space: my_space
externalId: MyContainer
properties:
  name:
    type:
      list: false
      collation: ucs_basic
      type: text
    immutable: false
    nullable: true
    autoIncrement: false
"""

VIEW_YAML = """space: my_space
externalId: MyView
version: v1
properties:
  name:
    container:
      type: container
      space: my_space
      externalId: MyContainer
    containerPropertyIdentifier: name
"""

DATA_MODEL_YAML = """space: my_space
externalId: MyModel
version: v1
views:
  - type: view
    space: my_space
    externalId: MyView
    version: v1
"""


def _container_property() -> ContainerPropertyDefinition:
    return ContainerPropertyDefinition(
        type=TextProperty(list=False, collation="ucs_basic"),
        immutable=False,
        nullable=True,
        auto_increment=False,
    )


def _cdf_container(property_identifiers: list[str]) -> ContainerResponse:
    return ContainerResponse(
        space=CONTAINER_ID.space,
        external_id=CONTAINER_ID.external_id,
        last_updated_time=0,
        created_time=0,
        description=None,
        name=None,
        used_for="node",
        is_global=False,
        properties={identifier: _container_property() for identifier in property_identifiers},
        indexes={},
        constraints={},
    )


def _cdf_view(property_identifiers: list[str], description: str | None = None) -> ViewResponse:
    # The API omits unset fields, and dumps use exclude_unset, so only pass description when set.
    optional_fields = {"description": description} if description is not None else {}
    return ViewResponse(
        space=VIEW_ID.space,
        external_id=VIEW_ID.external_id,
        version=VIEW_ID.version,
        **optional_fields,
        last_updated_time=0,
        created_time=0,
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
        mapped_containers=[CONTAINER_ID],
        properties={
            identifier: ViewCorePropertyResponse(
                container=CONTAINER_ID,
                container_property_identifier=identifier,
                type=TextProperty(),
                nullable=True,
                auto_increment=False,
                immutable=False,
                constraint_state=ConstraintOrIndexState(),
            )
            for identifier in property_identifiers
        },
    )


def _cdf_data_model(views: list[tuple[str, str]]) -> DataModelResponse:
    return DataModelResponse(
        space=DATA_MODEL_ID.space,
        external_id=DATA_MODEL_ID.external_id,
        version=DATA_MODEL_ID.version,
        last_updated_time=0,
        created_time=0,
        is_global=False,
        views=[ViewId(space=VIEW_ID.space, external_id=external_id, version=version) for external_id, version in views],
    )


def _built_module(yaml_file: Path, crud_cls: type[ResourceIO], identifier: Identifier) -> BuiltModule:
    module_id = ModuleId(id=RelativeDirPath(Path("modules/my")), path=yaml_file.parent.resolve())
    return BuiltModule(
        module_id=module_id,
        yaml_line_count=0,
        resources=[
            BuiltResource(
                identifier=identifier,
                source_hash="test-hash",
                type=ResourceType(resource_folder=crud_cls.folder_name, kind=crud_cls.kind),
                source_path=AbsoluteFilePath(yaml_file.resolve()),
                build_path=AbsoluteFilePath(yaml_file.resolve()),
                crud_cls=crud_cls,
                has_syntax_error=False,
                module_id=module_id,
            )
        ],
    )


def _client_stub(
    container: list[ContainerResponse] | None = None,
    view: list[ViewResponse] | None = None,
    data_model: list[DataModelResponse] | None = None,
) -> MagicMock:
    client = MagicMock()
    client.tool.containers.retrieve.return_value = container or []
    client.tool.views.retrieve.return_value = view or []
    client.tool.data_models.retrieve.return_value = data_model or []
    return client


class TestDependencyRuleSetDataModelingChanges:
    @pytest.mark.parametrize(
        "cdf_property_identifiers, expected_codes",
        [
            pytest.param(["name"], [], id="no-change"),
            pytest.param(
                ["name", "description"],
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                id="property-removed-locally",
            ),
        ],
    )
    def test_validate_container(
        self, tmp_path: Path, cdf_property_identifiers: list[str], expected_codes: list[str]
    ) -> None:
        yaml_file = tmp_path / "MyContainer.container.yaml"
        yaml_file.write_text(CONTAINER_YAML)

        client = _client_stub(container=[_cdf_container(cdf_property_identifiers)])
        rule = DependencyRuleSet(modules=[_built_module(yaml_file, ContainerCRUD, CONTAINER_ID)], client=client)

        insights = list(rule.validate())
        assert [insight.code for insight in insights] == expected_codes
        assert all(isinstance(insight, ConsistencyError) for insight in insights)

    @pytest.mark.parametrize(
        "cdf_property_identifiers, cdf_description, expected_codes, expected_message_fragment",
        [
            pytest.param(["name"], None, [], None, id="no-change"),
            pytest.param(
                ["name", "description"],
                None,
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "is missing properties 'description'",
                id="property-removed-locally",
            ),
            pytest.param(
                ["name"],
                "Deployed description",
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "has drifted from the state of the deployed view version",
                id="other-change-without-version-bump",
            ),
        ],
    )
    def test_validate_view(
        self,
        tmp_path: Path,
        cdf_property_identifiers: list[str],
        cdf_description: str | None,
        expected_codes: list[str],
        expected_message_fragment: str | None,
    ) -> None:
        yaml_file = tmp_path / "MyView.view.yaml"
        yaml_file.write_text(VIEW_YAML)

        client = _client_stub(view=[_cdf_view(cdf_property_identifiers, cdf_description)])
        rule = DependencyRuleSet(modules=[_built_module(yaml_file, ViewIO, VIEW_ID)], client=client)

        insights = list(rule.validate())
        assert [insight.code for insight in insights] == expected_codes
        if expected_message_fragment is not None:
            assert expected_message_fragment in insights[0].message

    @pytest.mark.parametrize(
        "cdf_views, expected_codes, expected_message_fragment",
        [
            pytest.param([("MyView", "v1")], [], None, id="no-change"),
            pytest.param(
                [("MyView", "v1"), ("OtherView", "v1")],
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "is missing views",
                id="view-removed-locally",
            ),
            pytest.param(
                [("MyView", "v0")],
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "has changed the view version of 'my_space:MyView' from 'v0' to 'v1'",
                id="view-version-changed-without-data-model-bump",
            ),
        ],
    )
    def test_validate_data_model(
        self,
        tmp_path: Path,
        cdf_views: list[tuple[str, str]],
        expected_codes: list[str],
        expected_message_fragment: str | None,
    ) -> None:
        yaml_file = tmp_path / "MyModel.datamodel.yaml"
        yaml_file.write_text(DATA_MODEL_YAML)

        client = _client_stub(data_model=[_cdf_data_model(cdf_views)])
        rule = DependencyRuleSet(modules=[_built_module(yaml_file, DataModelIO, DATA_MODEL_ID)], client=client)

        insights = list(rule.validate())
        assert [insight.code for insight in insights] == expected_codes
        if expected_message_fragment is not None:
            assert expected_message_fragment in insights[0].message

    def test_validate_skipped_without_client(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "MyContainer.container.yaml"
        yaml_file.write_text(CONTAINER_YAML)

        rule = DependencyRuleSet(modules=[_built_module(yaml_file, ContainerCRUD, CONTAINER_ID)])
        assert list(rule.validate()) == []

    def test_get_status_mentions_data_modeling_changes_with_client(self) -> None:
        rule = DependencyRuleSet(modules=[], client=MagicMock())
        assert rule.get_status().code == "ready"
        assert "state changes" in (rule.get_status().message or "")
