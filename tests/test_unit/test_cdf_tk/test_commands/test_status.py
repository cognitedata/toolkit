import io
from collections.abc import Iterable, Sequence, Sized
from pathlib import Path
from typing import Any, ClassVar, Literal
from unittest.mock import MagicMock

import pytest
import yaml
from rich.console import Console

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import AclType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId, ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import (
    AbsoluteDirPath,
    AbsoluteFilePath,
    RelativeDirPath,
)
from cognite_toolkit._cdf_tk.commands.status import ResourceKey, StatusCommand
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO


class _FakeCRUD(ResourceIO[ExternalId, dict[str, Any], dict[str, Any]]):
    folder_name = "fake_resources"
    kind = "Fake"
    existing_by_external_id: ClassVar[dict[str, dict[str, Any]]] = {}

    @classmethod
    def get_id(cls, item: ExternalId | dict[str, Any]) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        return ExternalId(external_id=item["externalId"])

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_minimum_scope(cls, items: Sequence[dict[str, Any]]) -> ScopeDefinition | None:
        return None

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        return []

    def load_resource_file(self, filepath: Path, environment_variables: dict[str, str | None] | None = None) -> list:
        return [yaml.safe_load(filepath.read_text())]

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> dict[str, Any]:
        return resource

    def dump_resource(self, resource: dict[str, Any], local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource

    def create(self, items: Sequence[dict[str, Any]]) -> Sized:
        return items

    def retrieve(self, ids: Sequence[ExternalId]) -> Sequence[dict[str, Any]]:
        return [
            self.existing_by_external_id[id_.external_id]
            for id_ in ids
            if id_.external_id in self.existing_by_external_id
        ]

    def delete(self, ids: Sequence[ExternalId]) -> int:
        return 0

    def _iterate(
        self, data_set_external_id: str | None = None, space: str | None = None, parent_ids: Sequence | None = None
    ) -> Iterable[dict[str, Any]]:
        return []


class _DependencyCRUD(_FakeCRUD):
    folder_name = "dependency_resources"
    kind = "Dependency"
    existing_by_external_id: ClassVar[dict[str, dict[str, Any]]] = {}


def _built_resource(
    tmp_path: Path,
    crud_cls: type[ResourceIO],
    external_id: str,
    value: str,
    dependencies: set[ResourceKey] | None = None,
) -> BuiltResource:
    source_file = tmp_path / "modules" / "my" / crud_cls.folder_name / f"{external_id}.{crud_cls.kind}.yaml"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text(yaml.safe_dump({"externalId": external_id, "value": value}))
    build_file = tmp_path / "build" / crud_cls.folder_name / f"{external_id}.{crud_cls.kind}.yaml"
    build_file.parent.mkdir(parents=True, exist_ok=True)
    build_file.write_text(yaml.safe_dump({"externalId": external_id, "value": value}))
    return BuiltResource(
        identifier=ExternalId(external_id=external_id),
        source_hash=f"hash-{external_id}",
        type=ResourceType(resource_folder=crud_cls.folder_name, kind=crud_cls.kind),
        source_path=AbsoluteFilePath(source_file.resolve()),
        build_path=AbsoluteFilePath(build_file.resolve()),
        crud_cls=crud_cls,
        dependencies=dependencies or set(),
        has_syntax_error=False,
    )


def test_build_graph_sets_statuses_and_dependency_flags(tmp_path: Path) -> None:
    _FakeCRUD.existing_by_external_id = {
        "unchanged": {"externalId": "unchanged", "value": "same"},
        "changed": {"externalId": "changed", "value": "old"},
    }
    _DependencyCRUD.existing_by_external_id = {
        "missing-local-existing-cdf": {"externalId": "missing-local-existing-cdf", "value": "cdf"},
        "local-dependency": {"externalId": "local-dependency", "value": "same"},
    }
    local_dependency_id = ExternalId(external_id="local-dependency")
    cdf_dependency_id = ExternalId(external_id="missing-local-existing-cdf")

    module_dir = tmp_path / "modules" / "my"
    module_dir.mkdir(parents=True, exist_ok=True)
    module = BuiltModule(
        module_id=ModuleId(id=RelativeDirPath(Path("modules/my")), path=AbsoluteDirPath(module_dir.resolve())),
        resources=[
            _built_resource(tmp_path, _FakeCRUD, "new", "new", {(_DependencyCRUD, local_dependency_id)}),
            _built_resource(tmp_path, _FakeCRUD, "unchanged", "same"),
            _built_resource(tmp_path, _FakeCRUD, "changed", "new", {(_DependencyCRUD, cdf_dependency_id)}),
            _built_resource(tmp_path, _DependencyCRUD, "local-dependency", "same"),
        ],
        yaml_line_count=8,
    )
    client = MagicMock()
    client.console = MagicMock()
    env_vars = MagicMock()
    env_vars.dump.return_value = {}

    graph = StatusCommand.build_graph([module], client, env_vars)

    resources_by_id = {resource.identifier.external_id: resource for resource in graph.resources}
    assert resources_by_id["new"].status == "new"
    assert resources_by_id["unchanged"].status == "unchanged"
    assert resources_by_id["changed"].status == "changed"

    local_dependency = resources_by_id["new"].dependencies[0]
    assert local_dependency.identifier == local_dependency_id
    assert local_dependency.in_config is True
    assert local_dependency.in_cdf is True

    cdf_dependency = resources_by_id["changed"].dependencies[0]
    assert cdf_dependency.identifier == cdf_dependency_id
    assert cdf_dependency.in_config is False
    assert cdf_dependency.in_cdf is True


def test_execute_hides_build_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    output = io.StringIO()
    client = MagicMock()
    client.console = Console(file=output, markup=True)
    env_vars = MagicMock()
    env_vars.get_client.return_value = client

    def noisy_build(self: Any, parameters: BuildParameters, client: Any) -> MagicMock:
        assert client is None
        assert parameters.user_selected_modules == ["modules/"]
        print("hidden stdout from build")
        self.console("hidden console from build")
        return MagicMock(built_modules=[])

    monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.status.BuildV2Command.build", noisy_build)

    StatusCommand(print_warning=False, skip_tracking=True).execute(
        env_vars=env_vars,
        organization_dir=tmp_path,
        config_yaml=None,
        selected=None,
        output_format="tree",
        verbose=False,
        client=client,
    )

    captured = capsys.readouterr()
    assert "hidden stdout from build" not in captured.out
    assert "hidden stdout from build" not in captured.err
    assert "hidden console from build" not in output.getvalue()
    assert "CDF status" in output.getvalue()


def test_build_graph_marks_cdf_values_unknown_without_client(tmp_path: Path) -> None:
    dependency_id = ExternalId(external_id="missing-local")
    module_dir = tmp_path / "modules" / "my"
    module_dir.mkdir(parents=True, exist_ok=True)
    module = BuiltModule(
        module_id=ModuleId(id=RelativeDirPath(Path("modules/my")), path=AbsoluteDirPath(module_dir.resolve())),
        resources=[
            _built_resource(tmp_path, _FakeCRUD, "local-resource", "value", {(_DependencyCRUD, dependency_id)}),
        ],
        yaml_line_count=2,
    )
    env_vars = MagicMock()
    env_vars.dump.return_value = {}

    graph = StatusCommand.build_graph([module], client=None, env_vars=env_vars)

    resource = graph.resources[0]
    assert resource.status == "unknown"
    assert resource.dependencies[0].in_config is False
    assert resource.dependencies[0].in_cdf == "unknown"
