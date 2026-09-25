from pathlib import Path
from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ExternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerResponse,
    DataModelResponse,
    Int32Property,
    TextProperty,
    ViewCorePropertyRequest,
    ViewCorePropertyResponse,
    ViewRequestProperty,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view_property import (
    ConstraintOrIndexState,
    SingleEdgeProperty,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId, ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath, RelativeDirPath
from cognite_toolkit._cdf_tk.resource_ios import (
    ContainerCRUD,
    DataModelIO,
    DataSetsIO,
    ResourceIO,
    TimeSeriesCRUD,
    ViewIO,
)
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet

CONTAINER_ID = ContainerId(space="my_space", external_id="MyContainer")
VIEW_ID = ViewId(space="my_space", external_id="MyView", version="v1")
DATA_MODEL_ID = DataModelId(space="my_space", external_id="MyModel", version="v1")


class TestIsDisallowedContainerPropertyChange:
    @pytest.mark.parametrize(
        "local_property, cdf_property, expected",
        [
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty(), nullable=True),
                ContainerPropertyDefinition(type=TextProperty(), nullable=True),
                False,
                id="no-change",
            ),
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty(), nullable=True),
                ContainerPropertyDefinition(type=TextProperty(), nullable=False),
                True,
                id="non-nullable-to-nullable-is-disallowed",
            ),
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty(), nullable=False),
                ContainerPropertyDefinition(type=TextProperty(), nullable=True),
                False,
                id="nullable-to-non-nullable-is-allowed",
            ),
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty()),
                ContainerPropertyDefinition(type=Int32Property()),
                True,
                id="type-change-is-disallowed",
            ),
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty(), auto_increment=True),
                ContainerPropertyDefinition(type=TextProperty(), auto_increment=False),
                True,
                id="auto-increment-change-is-disallowed",
            ),
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty(), name="a"),
                ContainerPropertyDefinition(type=TextProperty(), name="b"),
                False,
                id="metadata-change-is-allowed",
            ),
            pytest.param(
                ContainerPropertyDefinition(type=TextProperty()),
                ContainerPropertyDefinition(type=TextProperty(list=True, collation="en")),
                False,
                id="type-fields-left-unset-locally-are-not-flagged",
            ),
        ],
    )
    def test_is_disallowed(
        self, local_property: ContainerPropertyDefinition, cdf_property: ContainerPropertyDefinition, expected: bool
    ) -> None:
        assert DependencyRuleSet._is_disallowed_container_property_change(local_property, cdf_property) is expected


class TestIsDisallowedViewPropertyChange:
    @pytest.mark.parametrize(
        "local_property, cdf_property, expected",
        [
            pytest.param(
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="name"),
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="name"),
                False,
                id="no-change",
            ),
            pytest.param(
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="name"),
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="other"),
                False,
                id="base-property-container-mapping-change-is-allowed",
            ),
            pytest.param(
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="name", name="a"),
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="name", name="b"),
                False,
                id="base-property-metadata-change-is-allowed",
            ),
            pytest.param(
                SingleEdgeProperty(source=VIEW_ID, type=NodeId(space="my_space", external_id="myEdgeType"), name="a"),
                SingleEdgeProperty(source=VIEW_ID, type=NodeId(space="my_space", external_id="myEdgeType"), name="b"),
                False,
                id="connection-property-metadata-change-is-allowed",
            ),
            pytest.param(
                SingleEdgeProperty(
                    source=VIEW_ID, type=NodeId(space="my_space", external_id="myEdgeType"), direction="outwards"
                ),
                SingleEdgeProperty(
                    source=VIEW_ID, type=NodeId(space="my_space", external_id="myEdgeType"), direction="inwards"
                ),
                True,
                id="connection-property-direction-change-is-disallowed",
            ),
            pytest.param(
                SingleEdgeProperty(source=VIEW_ID, type=NodeId(space="my_space", external_id="myEdgeType")),
                SingleEdgeProperty(
                    source=ViewId(space="my_space", external_id="OtherView", version="v1"),
                    type=NodeId(space="my_space", external_id="myEdgeType"),
                ),
                True,
                id="connection-property-source-change-is-disallowed",
            ),
            pytest.param(
                ViewCorePropertyRequest(container=CONTAINER_ID, container_property_identifier="name"),
                SingleEdgeProperty(source=VIEW_ID, type=NodeId(space="my_space", external_id="myEdgeType")),
                True,
                id="property-kind-change-is-disallowed",
            ),
        ],
    )
    def test_is_disallowed(
        self, local_property: ViewRequestProperty, cdf_property: ViewRequestProperty, expected: bool
    ) -> None:
        assert DependencyRuleSet._is_disallowed_view_property_change(local_property, cdf_property) is expected


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

CONTAINER_YAML_WITH_EXTRA_PROPERTY = """space: my_space
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
  extra:
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

VIEW_YAML_WITH_EDGE_PROPERTY = """space: my_space
externalId: MyView
version: v1
properties:
  name:
    container:
      type: container
      space: my_space
      externalId: MyContainer
    containerPropertyIdentifier: name
  myEdge:
    connectionType: single_edge_connection
    source:
      type: view
      space: my_space
      externalId: OtherView
      version: v1
    type:
      space: my_space
      externalId: myEdgeType
    direction: outwards
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


def _container_property(nullable: bool = True) -> ContainerPropertyDefinition:
    return ContainerPropertyDefinition(
        type=TextProperty(list=False, collation="ucs_basic"),
        immutable=False,
        nullable=nullable,
        auto_increment=False,
    )


def _cdf_container(
    properties: dict[str, ContainerPropertyDefinition],
    description: str | None = None,
    used_for: Literal["node", "edge", "record", "all"] = "node",
) -> ContainerResponse:
    return ContainerResponse(
        space=CONTAINER_ID.space,
        external_id=CONTAINER_ID.external_id,
        last_updated_time=0,
        created_time=0,
        description=description,
        name=None,
        used_for=used_for,
        is_global=False,
        properties=properties,
        indexes={},
        constraints={},
    )


def _view_property(container_property_identifier: str = "name") -> ViewCorePropertyResponse:
    return ViewCorePropertyResponse(
        container=CONTAINER_ID,
        container_property_identifier=container_property_identifier,
        type=TextProperty(),
        nullable=True,
        auto_increment=False,
        immutable=False,
        constraint_state=ConstraintOrIndexState(),
    )


def _cdf_edge_property(direction: Literal["outwards", "inwards"] = "outwards") -> SingleEdgeProperty:
    return SingleEdgeProperty(
        source=ViewId(space="my_space", external_id="OtherView", version="v1"),
        type=NodeId(space="my_space", external_id="myEdgeType"),
        direction=direction,
    )


def _cdf_view(
    properties: dict[str, ViewCorePropertyResponse | SingleEdgeProperty],
    description: str | None = None,
    implements: list[ViewId] | None = None,
) -> ViewResponse:
    # The API omits unset fields, and dumps use exclude_unset, so only pass description/implements when set.
    optional_fields: dict[str, Any] = {}
    if description is not None:
        optional_fields["description"] = description
    if implements is not None:
        optional_fields["implements"] = implements
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
        properties=properties,
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
    """Integration-level tests: verifying that the CRUD wiring, aggregation and message/code plumbing in
    ``_validate_data_modeling_changes`` behaves correctly. Edge cases of the underlying predicates are
    covered by ``TestIsDisallowedContainerPropertyChange``/``TestIsDisallowedViewPropertyChange`` instead."""

    @pytest.mark.parametrize(
        "local_yaml, cdf_properties, cdf_description, cdf_used_for, expected_codes, expected_message_fragment",
        [
            pytest.param(CONTAINER_YAML, {"name": _container_property()}, None, "node", [], None, id="no-change"),
            pytest.param(
                CONTAINER_YAML,
                {"name": _container_property(), "description": _container_property()},
                None,
                "node",
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "is missing properties 'description'",
                id="property-removed-locally-is-disallowed",
            ),
            pytest.param(
                CONTAINER_YAML_WITH_EXTRA_PROPERTY,
                {"name": _container_property()},
                None,
                "node",
                [],
                None,
                id="property-added-locally-is-allowed",
            ),
            pytest.param(
                CONTAINER_YAML,
                {"name": _container_property(nullable=False), "description": _container_property()},
                None,
                "node",
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "has some properties 'description' and 'name' that have been modified",
                id="property-changed-and-removed-locally-reports-as-changed",
            ),
            pytest.param(
                CONTAINER_YAML,
                {"name": _container_property()},
                "Deployed description",
                "node",
                [],
                None,
                id="container-description-change-is-allowed",
            ),
            pytest.param(
                CONTAINER_YAML,
                {"name": _container_property()},
                None,
                "edge",
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "has modified usedFor",
                id="container-used-for-change-is-disallowed",
            ),
        ],
    )
    def test_validate_container(
        self,
        tmp_path: Path,
        local_yaml: str,
        cdf_properties: dict[str, ContainerPropertyDefinition],
        cdf_description: str | None,
        cdf_used_for: Literal["node", "edge", "record", "all"],
        expected_codes: list[str],
        expected_message_fragment: str | None,
    ) -> None:
        yaml_file = tmp_path / "MyContainer.container.yaml"
        yaml_file.write_text(local_yaml)

        client = _client_stub(
            container=[_cdf_container(cdf_properties, description=cdf_description, used_for=cdf_used_for)]
        )
        rule = DependencyRuleSet(modules=[_built_module(yaml_file, ContainerCRUD, CONTAINER_ID)], client=client)

        insights = list(rule.validate())
        assert [insight.code for insight in insights] == expected_codes
        assert all(isinstance(insight, ConsistencyError) for insight in insights)
        if expected_message_fragment is not None:
            assert expected_message_fragment in insights[0].message

    @pytest.mark.parametrize(
        "local_yaml, cdf_properties, cdf_description, cdf_implements, expected_codes, expected_message_fragment",
        [
            pytest.param(VIEW_YAML, {"name": _view_property()}, None, None, [], None, id="no-change"),
            pytest.param(
                VIEW_YAML,
                {"name": _view_property()},
                None,
                [],
                [],
                None,
                id="implements-empty-in-cdf-and-omitted-locally-is-no-change",
            ),
            pytest.param(
                VIEW_YAML,
                {"name": _view_property(), "description": _view_property()},
                None,
                None,
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "is missing properties 'description'",
                id="property-removed-locally",
            ),
            pytest.param(
                VIEW_YAML,
                {},
                None,
                None,
                [],
                None,
                id="property-added-locally-is-allowed",
            ),
            pytest.param(
                VIEW_YAML,
                {"name": _view_property()},
                "Deployed description",
                None,
                [],
                None,
                id="view-description-change-is-allowed",
            ),
            pytest.param(
                VIEW_YAML,
                {"name": _view_property()},
                None,
                [ViewId(space="my_space", external_id="ParentView", version="v1")],
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "has changed implements",
                id="view-implements-change-is-disallowed",
            ),
            pytest.param(
                VIEW_YAML_WITH_EDGE_PROPERTY,
                {
                    "name": _view_property(),
                    "description": _view_property(),
                    "myEdge": _cdf_edge_property(direction="inwards"),
                },
                None,
                None,
                [DependencyRuleSet.INVALID_OPERATION_CODE],
                "has some properties 'description' and 'myEdge' that have been modified",
                id="connection-property-changed-and-base-property-removed-reports-as-changed",
            ),
        ],
    )
    def test_validate_view(
        self,
        tmp_path: Path,
        local_yaml: str,
        cdf_properties: dict[str, ViewCorePropertyResponse | SingleEdgeProperty],
        cdf_description: str | None,
        cdf_implements: list[ViewId] | None,
        expected_codes: list[str],
        expected_message_fragment: str | None,
    ) -> None:
        yaml_file = tmp_path / "MyView.view.yaml"
        yaml_file.write_text(local_yaml)

        client = _client_stub(view=[_cdf_view(cdf_properties, cdf_description, cdf_implements)])
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
                "is missing the view(s)",
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


class TestDependencyRuleSetCdfApiError:
    def test_retrieve_api_error_is_reported_instead_of_raised(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "my.TimeSeries.yaml"
        yaml_file.write_text("externalId: my_ts\n")
        dataset_id = ExternalId(external_id="my_dataset")
        module = _built_module(yaml_file, TimeSeriesCRUD, ExternalId(external_id="my_ts"))
        module.resources[0].dependencies.add((DataSetsIO, dataset_id))

        client = MagicMock()
        client.tool.datasets.retrieve.side_effect = ToolkitAPIError("403 Forbidden", code=403)
        rule = DependencyRuleSet(modules=[module], client=client)

        results = list(rule.validate())

        assert [(type(result), result.source, result.message) for result in results] == [
            (
                InternalValidatorException,
                "DataSet",
                "Failed to verify existence of dataset in CDF: 403 Forbidden",
            )
        ]
