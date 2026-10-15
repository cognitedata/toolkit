from pathlib import Path
from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerResponse,
    DataModelResponse,
    DirectNodeRelation,
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
    BuildError,
    BuildWarning,
    InsightDefinition,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath, RelativeDirPath
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, DataModelIO, ResourceIO, ResourceType, ViewIO
from cognite_toolkit._cdf_tk.rules._data_modeling import DataModelingRuleSet
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet

CONTAINER_ID = ContainerId(space="my_space", external_id="MyContainer")
VIEW_ID = ViewId(space="my_space", external_id="MyView", version="v1")
OTHER_VIEW_ID = ViewId(space="my_space", external_id="OtherView", version="v1")
DATA_MODEL_ID = DataModelId(space="my_space", external_id="MyModel", version="v1")

CONTAINER_YAML = """space: my_space
externalId: MyContainer
properties:
  name:
    type:
      type: text
"""

DIRECT_CONTAINER_YAML = """space: my_space
externalId: MyContainer
properties:
  related:
    type:
      type: direct
"""

TEXT_CONTAINER_YAML = """space: my_space
externalId: MyContainer
properties:
  related:
    type:
      type: text
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

MAPPED_VIEW_YAML = """space: my_space
externalId: OtherView
version: v1
properties:
  related:
    container:
      type: container
      space: my_space
      externalId: MyContainer
    containerPropertyIdentifier: related
"""

REVERSE_THROUGH_VIEW_YAML = """space: my_space
externalId: MyView
version: v1
properties:
  back:
    connectionType: single_reverse_direct_relation
    source:
      type: view
      space: my_space
      externalId: OtherView
      version: v1
    through:
      source:
        type: view
        space: my_space
        externalId: OtherView
        version: v1
      identifier: related
"""

CONTAINER_REMOVED_PROPERTY_YAML = """space: my_space
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


def _write(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content)
    return path


def _module(resources: list[tuple[Path, type[ResourceIO], ContainerId | ViewId]]) -> BuiltModule:
    module_id = ModuleId(id=RelativeDirPath(Path("modules/my")), path=resources[0][0].parent.resolve())
    built = [
        BuiltResource(
            identifier=identifier,
            source_hash="test-hash",
            type=ResourceType(resource_folder=crud_cls.folder_name, kind=crud_cls.kind),
            source_path=AbsoluteFilePath(path.resolve()),
            build_path=AbsoluteFilePath(path.resolve()),
            crud_cls=crud_cls,
            has_syntax_error=False,
            module_id=module_id,
        )
        for path, crud_cls, identifier in resources
    ]
    return BuiltModule(module_id=module_id, yaml_line_count=0, resources=built)


def _client(
    containers: list[ContainerResponse] | None = None,
    views: list[ViewResponse] | None = None,
    data_models: list[DataModelResponse] | None = None,
) -> MagicMock:
    client = MagicMock()
    client.containers.retrieve.return_value = containers or []
    client.views.retrieve.return_value = views or []
    client.data_models.retrieve.return_value = data_models or []
    return client


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
        used_for=used_for,
        is_global=False,
        properties=properties,
    )


def _cdf_view(view_id: ViewId, properties: dict[str, Any]) -> ViewResponse:
    return ViewResponse(
        space=view_id.space,
        external_id=view_id.external_id,
        version=view_id.version,
        last_updated_time=0,
        created_time=0,
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
        mapped_containers=[],
        properties=properties,
    )


def _view_property(container_property_identifier: str, direct: bool) -> ViewCorePropertyResponse:
    return ViewCorePropertyResponse(
        container=CONTAINER_ID,
        container_property_identifier=container_property_identifier,
        type=DirectNodeRelation() if direct else TextProperty(),
        constraint_state=ConstraintOrIndexState(),
    )


class TestDataModelingRuleSetStatus:
    def test_get_status_reduced_without_client(self) -> None:
        status = DataModelingRuleSet(modules=[]).get_status()
        assert (status.code, "within the provided modules" in (status.message or "")) == ("reduced", True)

    def test_get_status_ready_with_client(self) -> None:
        status = DataModelingRuleSet(modules=[], client=MagicMock()).get_status()
        assert (status.code, "state changes" in (status.message or "")) == ("ready", True)


class TestContainerPropertyReferences:
    def test_local_container_property_is_accepted(self, tmp_path: Path) -> None:
        container_file = _write(tmp_path, "MyContainer.container.yaml", CONTAINER_YAML)
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        rule = DataModelingRuleSet(
            modules=[
                _module(
                    [
                        (container_file, ContainerIO, CONTAINER_ID),
                        (view_file, ViewIO, VIEW_ID),
                    ]
                )
            ]
        )

        assert list(rule.validate()) == []

    def test_missing_local_property_without_client_is_unverified(self, tmp_path: Path) -> None:
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        rule = DataModelingRuleSet(modules=[_module([(view_file, ViewIO, VIEW_ID)])])

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [(insight.code, "my_space:MyContainer.name" in insight.message) for insight in insights] == [
            ("UNVERIFIED-REFERENCED-PROPERTY", True)
        ]

    def test_property_found_in_cdf_is_accepted(self, tmp_path: Path) -> None:
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        client = _client(containers=[_cdf_container({"name": ContainerPropertyDefinition(type=TextProperty())})])
        rule = DataModelingRuleSet(modules=[_module([(view_file, ViewIO, VIEW_ID)])], client=client)

        assert list(rule.validate()) == []

    def test_property_missing_in_cdf_is_unknown(self, tmp_path: Path) -> None:
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        client = _client(containers=[_cdf_container({})])
        rule = DataModelingRuleSet(modules=[_module([(view_file, ViewIO, VIEW_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [(insight.code, "my_space:MyContainer.name" in insight.message) for insight in insights] == [
            ("MISSING-REFERENCED-PROPERTY", True)
        ]

    def test_container_retrieve_error_is_reported(self, tmp_path: Path) -> None:
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        client = _client()
        client.containers.retrieve.side_effect = ToolkitAPIError("403 Forbidden", code=403)
        rule = DataModelingRuleSet(modules=[_module([(view_file, ViewIO, VIEW_ID)])], client=client)

        results = list(rule.validate())
        assert [(type(result), getattr(result, "source", None)) for result in results] == [
            (InternalValidatorException, "Container")
        ]


class TestReverseDirectRelations:
    def test_local_view_direct_relation_is_accepted(self, tmp_path: Path) -> None:
        container_file = _write(tmp_path, "MyContainer.container.yaml", DIRECT_CONTAINER_YAML)
        mapped_view = _write(tmp_path, "OtherView.view.yaml", MAPPED_VIEW_YAML)
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        rule = DataModelingRuleSet(
            modules=[
                _module(
                    [
                        (container_file, ContainerIO, CONTAINER_ID),
                        (mapped_view, ViewIO, OTHER_VIEW_ID),
                        (reverse_view, ViewIO, VIEW_ID),
                    ]
                )
            ]
        )

        assert list(rule.validate()) == []

    def test_local_property_that_is_not_direct_is_rejected(self, tmp_path: Path) -> None:
        container_file = _write(tmp_path, "MyContainer.container.yaml", TEXT_CONTAINER_YAML)
        mapped_view = _write(tmp_path, "OtherView.view.yaml", MAPPED_VIEW_YAML)
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        rule = DataModelingRuleSet(
            modules=[
                _module(
                    [
                        (container_file, ContainerIO, CONTAINER_ID),
                        (mapped_view, ViewIO, OTHER_VIEW_ID),
                        (reverse_view, ViewIO, VIEW_ID),
                    ]
                )
            ]
        )

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [(insight.code, "not a direct relation" in insight.message) for insight in insights] == [
            ("INVALID-REFERENCED-PROPERTY", True)
        ]

    def test_missing_reverse_without_client_is_unverified(self, tmp_path: Path) -> None:
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        rule = DataModelingRuleSet(modules=[_module([(reverse_view, ViewIO, VIEW_ID)])])

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [(insight.code, "direct relation" in insight.message) for insight in insights] == [
            ("UNVERIFIED-REFERENCED-PROPERTY", True)
        ]

    def test_direct_relation_found_on_cdf_view_is_accepted(self, tmp_path: Path) -> None:
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        client = _client(views=[_cdf_view(OTHER_VIEW_ID, {"related": _view_property("related", direct=True)})])
        rule = DataModelingRuleSet(modules=[_module([(reverse_view, ViewIO, VIEW_ID)])], client=client)

        assert list(rule.validate()) == []

    def test_cdf_view_property_that_is_not_direct_is_rejected(self, tmp_path: Path) -> None:
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        client = _client(views=[_cdf_view(OTHER_VIEW_ID, {"related": _view_property("related", direct=False)})])
        rule = DataModelingRuleSet(modules=[_module([(reverse_view, ViewIO, VIEW_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [insight.code for insight in insights] == ["INVALID-REFERENCED-PROPERTY"]

    def test_reverse_missing_in_cdf_is_unknown(self, tmp_path: Path) -> None:
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        client = _client(views=[_cdf_view(OTHER_VIEW_ID, {})])
        rule = DataModelingRuleSet(modules=[_module([(reverse_view, ViewIO, VIEW_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [insight.code for insight in insights] == ["MISSING-REFERENCED-PROPERTY"]


class TestDataModelingChangesMove:
    def test_state_changes_run_here_and_are_skipped_by_dependency_rules(self, tmp_path: Path) -> None:
        container_file = _write(tmp_path, "MyContainer.container.yaml", CONTAINER_REMOVED_PROPERTY_YAML)
        module = _module([(container_file, ContainerIO, CONTAINER_ID)])
        deployed_property = ContainerPropertyDefinition(
            type=TextProperty(list=False, collation="ucs_basic"),
            immutable=False,
            nullable=True,
            auto_increment=False,
        )
        client = _client(containers=[_cdf_container({"name": deployed_property, "description": deployed_property})])

        data_modeling = [
            insight
            for insight in DataModelingRuleSet(modules=[module], client=client).validate()
            if isinstance(insight, InsightDefinition)
        ]
        dependencies = list(DependencyRuleSet(modules=[module], client=client).validate())

        assert (
            [insight.code for insight in data_modeling],
            "is missing properties 'description'" in data_modeling[0].message,
            dependencies,
        ) == (["UNSUPPORTED-RESOURCE-REMOVAL"], True, [])


CHANGE_CONTAINER_YAML = """space: my_space
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

CHANGE_CONTAINER_YAML_WITH_EXTRA_PROPERTY = """space: my_space
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

CHANGE_VIEW_YAML = """space: my_space
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

CHANGE_VIEW_YAML_WITH_EDGE_PROPERTY = """space: my_space
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


def _mapped_view_property(container_property_identifier: str = "name") -> ViewCorePropertyResponse:
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
        source=OTHER_VIEW_ID,
        type=NodeId(space="my_space", external_id="myEdgeType"),
        direction=direction,
    )


def _deployed_view(
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


def _referenced_container() -> ContainerResponse:
    """Container the change-detection view YAML maps to, so reference checks stay quiet."""
    return _cdf_container({"name": ContainerPropertyDefinition(type=TextProperty())})


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
        assert DataModelingRuleSet._is_disallowed_container_property_change(local_property, cdf_property) is expected


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
                    source=OTHER_VIEW_ID,
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
        assert DataModelingRuleSet._is_disallowed_view_property_change(local_property, cdf_property) is expected


class TestDataModelingStateChanges:
    """Container, view, and data model changes that CDF will not apply are reported by the data modeling rules."""

    @pytest.mark.parametrize(
        "local_yaml, cdf_properties, cdf_description, cdf_used_for, expected_codes, expected_message_fragment",
        [
            pytest.param(
                CHANGE_CONTAINER_YAML, {"name": _container_property()}, None, "node", [], None, id="no-change"
            ),
            pytest.param(
                CHANGE_CONTAINER_YAML,
                {"name": _container_property(), "description": _container_property()},
                None,
                "node",
                ["UNSUPPORTED-RESOURCE-REMOVAL"],
                "is missing properties 'description'",
                id="property-removed-locally-is-disallowed",
            ),
            pytest.param(
                CHANGE_CONTAINER_YAML_WITH_EXTRA_PROPERTY,
                {"name": _container_property()},
                None,
                "node",
                [],
                None,
                id="property-added-locally-is-allowed",
            ),
            pytest.param(
                CHANGE_CONTAINER_YAML,
                {"name": _container_property(nullable=False), "description": _container_property()},
                None,
                "node",
                ["INVALID-RESOURCE-CHANGE"],
                "has some properties 'description' and 'name' that have been modified",
                id="property-changed-and-removed-locally-reports-as-changed",
            ),
            pytest.param(
                CHANGE_CONTAINER_YAML,
                {"name": _container_property()},
                "Deployed description",
                "node",
                [],
                None,
                id="container-description-change-is-allowed",
            ),
            pytest.param(
                CHANGE_CONTAINER_YAML,
                {"name": _container_property()},
                None,
                "edge",
                ["INVALID-RESOURCE-CHANGE"],
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
        yaml_file = _write(tmp_path, "MyContainer.container.yaml", local_yaml)
        client = _client(
            containers=[_cdf_container(cdf_properties, description=cdf_description, used_for=cdf_used_for)]
        )
        rule = DataModelingRuleSet(modules=[_module([(yaml_file, ContainerIO, CONTAINER_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert (
            [insight.code for insight in insights],
            expected_message_fragment is None or expected_message_fragment in insights[0].message,
        ) == (expected_codes, True)

    @pytest.mark.parametrize(
        "cdf_properties, cdf_used_for, expected_insight_type",
        [
            pytest.param(
                {"name": _container_property(), "description": _container_property()},
                "node",
                BuildWarning,
                id="property-missing-locally-is-warning",
            ),
            pytest.param({"name": _container_property()}, "edge", BuildError, id="used-for-change-is-error"),
        ],
    )
    def test_validate_container_insight_type(
        self,
        tmp_path: Path,
        cdf_properties: dict[str, ContainerPropertyDefinition],
        cdf_used_for: Literal["node", "edge", "record", "all"],
        expected_insight_type: type[InsightDefinition],
    ) -> None:
        yaml_file = _write(tmp_path, "MyContainer.container.yaml", CHANGE_CONTAINER_YAML)
        client = _client(containers=[_cdf_container(cdf_properties, used_for=cdf_used_for)])
        rule = DataModelingRuleSet(modules=[_module([(yaml_file, ContainerIO, CONTAINER_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [type(insight) for insight in insights] == [expected_insight_type]

    @pytest.mark.parametrize(
        "local_yaml, cdf_properties, cdf_description, cdf_implements, expected_codes, expected_message_fragment",
        [
            pytest.param(CHANGE_VIEW_YAML, {"name": _mapped_view_property()}, None, None, [], None, id="no-change"),
            pytest.param(
                CHANGE_VIEW_YAML,
                {"name": _mapped_view_property()},
                None,
                [],
                [],
                None,
                id="implements-empty-in-cdf-and-omitted-locally-is-no-change",
            ),
            pytest.param(
                CHANGE_VIEW_YAML,
                {"name": _mapped_view_property(), "description": _mapped_view_property()},
                None,
                None,
                ["UNSUPPORTED-RESOURCE-REMOVAL"],
                "is missing properties 'description'",
                id="property-removed-locally",
            ),
            pytest.param(
                CHANGE_VIEW_YAML,
                {},
                None,
                None,
                [],
                None,
                id="property-added-locally-is-allowed",
            ),
            pytest.param(
                CHANGE_VIEW_YAML,
                {"name": _mapped_view_property()},
                "Deployed description",
                None,
                [],
                None,
                id="view-description-change-is-allowed",
            ),
            pytest.param(
                CHANGE_VIEW_YAML,
                {"name": _mapped_view_property()},
                None,
                [ViewId(space="my_space", external_id="ParentView", version="v1")],
                ["INVALID-RESOURCE-CHANGE"],
                "has changed implements",
                id="view-implements-change-is-disallowed",
            ),
            pytest.param(
                CHANGE_VIEW_YAML_WITH_EDGE_PROPERTY,
                {
                    "name": _mapped_view_property(),
                    "description": _mapped_view_property(),
                    "myEdge": _cdf_edge_property(direction="inwards"),
                },
                None,
                None,
                ["INVALID-RESOURCE-CHANGE"],
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
        yaml_file = _write(tmp_path, "MyView.view.yaml", local_yaml)
        client = _client(
            containers=[_referenced_container()],
            views=[_deployed_view(cdf_properties, cdf_description, cdf_implements)],
        )
        rule = DataModelingRuleSet(modules=[_module([(yaml_file, ViewIO, VIEW_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert (
            [insight.code for insight in insights],
            expected_message_fragment is None or expected_message_fragment in insights[0].message,
        ) == (expected_codes, True)

    @pytest.mark.parametrize(
        "cdf_views, expected_codes, expected_message_fragment",
        [
            pytest.param([("MyView", "v1")], [], None, id="no-change"),
            pytest.param(
                [("MyView", "v1"), ("OtherView", "v1")],
                ["UNSUPPORTED-RESOURCE-REMOVAL"],
                "is missing the view(s)",
                id="view-removed-locally",
            ),
            pytest.param(
                [("MyView", "v0")],
                ["INVALID-RESOURCE-CHANGE"],
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
        yaml_file = _write(tmp_path, "MyModel.datamodel.yaml", DATA_MODEL_YAML)
        client = _client(data_models=[_cdf_data_model(cdf_views)])
        rule = DataModelingRuleSet(modules=[_module([(yaml_file, DataModelIO, DATA_MODEL_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert (
            [insight.code for insight in insights],
            expected_message_fragment is None or expected_message_fragment in insights[0].message,
        ) == (expected_codes, True)

    def test_state_changes_are_skipped_without_client(self, tmp_path: Path) -> None:
        yaml_file = _write(tmp_path, "MyContainer.container.yaml", CHANGE_CONTAINER_YAML)
        rule = DataModelingRuleSet(modules=[_module([(yaml_file, ContainerIO, CONTAINER_ID)])])
        assert list(rule.validate()) == []
