from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerResponse,
    DirectNodeRelation,
    TextProperty,
    ViewCorePropertyResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view_property import ConstraintOrIndexState
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    InsightDefinition,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath, RelativeDirPath
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, ResourceIO, ResourceType, ViewIO
from cognite_toolkit._cdf_tk.rules._base import UNKNOWN_REFERENCE, UNVERIFIED_REFERENCE
from cognite_toolkit._cdf_tk.rules._data_modeling import DataModelingRuleSet
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet

CONTAINER_ID = ContainerId(space="my_space", external_id="MyContainer")
VIEW_ID = ViewId(space="my_space", external_id="MyView", version="v1")
OTHER_VIEW_ID = ViewId(space="my_space", external_id="OtherView", version="v1")

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


def _set_alpha_rules(monkeypatch: pytest.MonkeyPatch, enabled: bool) -> None:
    original = FeatureFlag.is_enabled.__wrapped__

    def _is_enabled(flag: Flags) -> bool:
        if flag is Flags.ALPHA_RULES:
            return enabled
        return original(flag)

    monkeypatch.setattr(FeatureFlag, "is_enabled", _is_enabled)


@pytest.fixture()
def alpha_rules_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_alpha_rules(monkeypatch, True)


@pytest.fixture()
def alpha_rules_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_alpha_rules(monkeypatch, False)


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
) -> MagicMock:
    client = MagicMock()
    client.tool.containers.retrieve.return_value = containers or []
    client.tool.views.retrieve.return_value = views or []
    client.tool.data_models.retrieve.return_value = []
    return client


def _cdf_container(properties: dict[str, ContainerPropertyDefinition]) -> ContainerResponse:
    return ContainerResponse(
        space=CONTAINER_ID.space,
        external_id=CONTAINER_ID.external_id,
        last_updated_time=0,
        created_time=0,
        used_for="node",
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


@pytest.mark.usefixtures("alpha_rules_disabled")
class TestAlphaRulesDisabled:
    def test_get_status_skip_when_alpha_rules_disabled(self) -> None:
        status = DataModelingRuleSet(modules=[], client=MagicMock()).get_status()
        assert status.code == "skip"

    def test_validate_is_skipped_when_alpha_rules_disabled(self, tmp_path: Path) -> None:
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        rule = DataModelingRuleSet(modules=[_module([(view_file, ViewIO, VIEW_ID)])])
        assert list(rule.validate()) == []


@pytest.mark.usefixtures("alpha_rules_enabled")
class TestAlphaRulesEnabled:
    def test_get_status_reduced_without_client(self) -> None:
        status = DataModelingRuleSet(modules=[]).get_status()
        assert (status.code, "within the provided modules" in (status.message or "")) == ("reduced", True)

    def test_get_status_ready_with_client(self) -> None:
        status = DataModelingRuleSet(modules=[], client=MagicMock()).get_status()
        assert (status.code, "state changes" in (status.message or "")) == ("ready", True)


@pytest.mark.usefixtures("alpha_rules_enabled")
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
            (UNVERIFIED_REFERENCE, True)
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
            (UNKNOWN_REFERENCE, True)
        ]

    def test_container_retrieve_error_is_reported(self, tmp_path: Path) -> None:
        view_file = _write(tmp_path, "MyView.view.yaml", VIEW_YAML)
        client = _client()
        client.tool.containers.retrieve.side_effect = ToolkitAPIError("403 Forbidden", code=403)
        rule = DataModelingRuleSet(modules=[_module([(view_file, ViewIO, VIEW_ID)])], client=client)

        results = list(rule.validate())
        assert [(type(result), getattr(result, "source", None)) for result in results] == [
            (InternalValidatorException, "Container")
        ]


@pytest.mark.usefixtures("alpha_rules_enabled")
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
            (DataModelingRuleSet.INVALID_REVERSE_DIRECT_RELATION, True)
        ]

    def test_missing_reverse_without_client_is_unverified(self, tmp_path: Path) -> None:
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        rule = DataModelingRuleSet(modules=[_module([(reverse_view, ViewIO, VIEW_ID)])])

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [(insight.code, "direct relation" in insight.message) for insight in insights] == [
            (UNVERIFIED_REFERENCE, True)
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
        assert [insight.code for insight in insights] == [DataModelingRuleSet.INVALID_REVERSE_DIRECT_RELATION]

    def test_reverse_missing_in_cdf_is_unknown(self, tmp_path: Path) -> None:
        reverse_view = _write(tmp_path, "MyView.view.yaml", REVERSE_THROUGH_VIEW_YAML)
        client = _client(views=[_cdf_view(OTHER_VIEW_ID, {})])
        rule = DataModelingRuleSet(modules=[_module([(reverse_view, ViewIO, VIEW_ID)])], client=client)

        insights = [insight for insight in rule.validate() if isinstance(insight, InsightDefinition)]
        assert [insight.code for insight in insights] == [UNKNOWN_REFERENCE]


@pytest.mark.usefixtures("alpha_rules_enabled")
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
        ) == ([DependencyRuleSet.CONTAINER_INVALID_OPERATION_CODE], True, [])
