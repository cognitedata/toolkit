from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId, ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import (
    AbsoluteDirPath,
    AbsoluteFilePath,
    RelativeDirPath,
)
from cognite_toolkit._cdf_tk.resource_ios import InFieldCDMLocationConfigIO
from cognite_toolkit._cdf_tk.rules._infield import InFieldCDMViewPropertiesRuleSet, _REQUIRED_PROPERTIES


@pytest.fixture
def mock_view() -> Callable[[ViewId, frozenset[str]], MagicMock]:
    def _mock_view(view_id: ViewId, property_names: frozenset[str]) -> MagicMock:
        view = MagicMock()
        view.properties = {name: MagicMock() for name in property_names}
        view.as_id.return_value = view_id
        return view

    return _mock_view


@pytest.fixture
def both_cards_config() -> dict:
    return {
        "space": "sp_instance",
        "externalId": "my_location_config",
        "dataExplorationConfig": {
            "assetActivitiesCard": {
                "space": "customer_idm",
                "version": "v2",
                "externalId": "ActivitiesCard",
            },
            "assetNotificationsCard": {
                "space": "customer_idm",
                "version": "v2",
                "externalId": "NotificationsCard",
            },
        },
    }


@pytest.fixture
def write_config_yaml() -> Callable[[Path, dict], None]:
    def _write(filepath: Path, content: dict) -> None:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            yaml.safe_dump(content, f)

    return _write


@pytest.fixture
def create_built_resource() -> Callable[[Path, Path], BuiltResource]:
    def _create(source_path: Path, build_path: Path) -> BuiltResource:
        return BuiltResource(
            identifier=NodeId(space="sp_instance", external_id="my_location_config"),
            source_hash="test-hash",
            type=ResourceType(
                resource_folder=InFieldCDMLocationConfigIO.folder_name,
                kind=InFieldCDMLocationConfigIO.kind,
            ),
            source_path=AbsoluteFilePath(source_path.resolve()),
            build_path=AbsoluteFilePath(build_path.resolve()),
            crud_cls=InFieldCDMLocationConfigIO,
            dependencies=set(),
            has_syntax_error=False,
        )

    return _create


@pytest.fixture
def create_module() -> Callable[[Path, list[BuiltResource]], BuiltModule]:
    def _create(tmp_path: Path, resources: list[BuiltResource]) -> BuiltModule:
        mod_path = tmp_path / "modules" / "my"
        mod_path.mkdir(parents=True, exist_ok=True)
        return BuiltModule(
            module_id=ModuleId(
                id=RelativeDirPath(Path("modules/my")),
                path=AbsoluteDirPath(mod_path.resolve()),
            ),
            resources=resources,
            yaml_line_count=1,
        )

    return _create


class TestInFieldCDMViewPropertiesRuleSet:
    def test_get_status_with_client(self) -> None:
        rule = InFieldCDMViewPropertiesRuleSet(modules=[], client=MagicMock())
        status = rule.get_status()
        assert status.code == "ready"

    def test_get_status_without_client(self) -> None:
        rule = InFieldCDMViewPropertiesRuleSet(modules=[])
        status = rule.get_status()
        assert status.code == "reduced"

    def test_valid_view_has_all_required_properties(
        self,
        tmp_path: Path,
        mock_view: Callable[[ViewId, frozenset[str]], MagicMock],
        both_cards_config: dict,
        write_config_yaml: Callable[[Path, dict], None],
        create_built_resource: Callable[[Path, Path], BuiltResource],
        create_module: Callable[[Path, list[BuiltResource]], BuiltModule],
    ) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        write_config_yaml(yaml_file, both_cards_config)
        resource = create_built_resource(yaml_file, yaml_file)
        module = create_module(tmp_path, [resource])

        activities_id = ViewId(space="customer_idm", external_id="ActivitiesCard", version="v2")
        notifications_id = ViewId(space="customer_idm", external_id="NotificationsCard", version="v2")
        view_map = {
            activities_id: mock_view(activities_id, _REQUIRED_PROPERTIES["assetActivitiesCard"]),
            notifications_id: mock_view(notifications_id, _REQUIRED_PROPERTIES["assetNotificationsCard"]),
        }

        mock_client = MagicMock()
        mock_client.tool.views.retrieve.side_effect = lambda ids, **_: [
            view_map[v] for v in ids if v in view_map
        ]
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        results = list(rule.validate())
        assert results == []
        mock_client.tool.views.retrieve.assert_called_once()

    def test_view_missing_required_properties(
        self,
        tmp_path: Path,
        mock_view: Callable[[ViewId, frozenset[str]], MagicMock],
        write_config_yaml: Callable[[Path, dict], None],
        create_built_resource: Callable[[Path, Path], BuiltResource],
        create_module: Callable[[Path, list[BuiltResource]], BuiltModule],
    ) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        write_config_yaml(
            yaml_file,
            {
                "space": "sp_instance",
                "externalId": "my_location_config",
                "dataExplorationConfig": {
                    "assetActivitiesCard": {
                        "space": "customer_idm",
                        "version": "v2",
                        "externalId": "ActivitiesCard",
                    },
                },
            },
        )
        resource = create_built_resource(yaml_file, yaml_file)
        module = create_module(tmp_path, [resource])
        activities_id = ViewId(space="customer_idm", external_id="ActivitiesCard", version="v2")
        activities_required = _REQUIRED_PROPERTIES["assetActivitiesCard"]
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = [
            mock_view(activities_id, activities_required - {"mainAsset"})
        ]
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        errors = list(rule.validate())
        assert len(errors) == 1
        assert errors[0].code == f"{InFieldCDMViewPropertiesRuleSet.CODE_PREFIX}-VIEW-MISSING-PROPERTIES"
        assert "mainAsset" in errors[0].message

    def test_view_not_found_in_cdf(
        self,
        tmp_path: Path,
        write_config_yaml: Callable[[Path, dict], None],
        create_built_resource: Callable[[Path, Path], BuiltResource],
        create_module: Callable[[Path, list[BuiltResource]], BuiltModule],
    ) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        write_config_yaml(
            yaml_file,
            {
                "space": "sp_instance",
                "externalId": "my_location_config",
                "dataExplorationConfig": {
                    "assetActivitiesCard": {
                        "space": "customer_idm",
                        "version": "v2",
                        "externalId": "ActivitiesCard",
                    },
                },
            },
        )
        resource = create_built_resource(yaml_file, yaml_file)
        module = create_module(tmp_path, [resource])
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = []
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        errors = list(rule.validate())
        assert len(errors) == 1
        assert errors[0].code == f"{InFieldCDMViewPropertiesRuleSet.CODE_PREFIX}-VIEW-NOT-FOUND"
        assert "was not found in CDF" in errors[0].message

    def test_retrieve_called_once_for_multiple_resources(
        self,
        tmp_path: Path,
        mock_view: Callable[[ViewId, frozenset[str]], MagicMock],
        both_cards_config: dict,
        write_config_yaml: Callable[[Path, dict], None],
        create_built_resource: Callable[[Path, Path], BuiltResource],
        create_module: Callable[[Path, list[BuiltResource]], BuiltModule],
    ) -> None:
        yaml_file_1 = tmp_path / "cdf_applications" / "loc1.InFieldCDMLocationConfig.yaml"
        yaml_file_2 = tmp_path / "cdf_applications" / "loc2.InFieldCDMLocationConfig.yaml"
        write_config_yaml(yaml_file_1, both_cards_config)
        write_config_yaml(yaml_file_2, both_cards_config)
        resources = [
            create_built_resource(yaml_file_1, yaml_file_1),
            create_built_resource(yaml_file_2, yaml_file_2),
        ]
        module = create_module(tmp_path, resources)

        activities_id = ViewId(space="customer_idm", external_id="ActivitiesCard", version="v2")
        notifications_id = ViewId(space="customer_idm", external_id="NotificationsCard", version="v2")
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = [
            mock_view(activities_id, _REQUIRED_PROPERTIES["assetActivitiesCard"]),
            mock_view(notifications_id, _REQUIRED_PROPERTIES["assetNotificationsCard"]),
        ]
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        list(rule.validate())
        mock_client.tool.views.retrieve.assert_called_once()
        call_view_ids = mock_client.tool.views.retrieve.call_args[0][0]
        assert set(call_view_ids) == {activities_id, notifications_id}

    def test_retrieve_batch_failure_propagates(
        self,
        tmp_path: Path,
        both_cards_config: dict,
        write_config_yaml: Callable[[Path, dict], None],
        create_built_resource: Callable[[Path, Path], BuiltResource],
        create_module: Callable[[Path, list[BuiltResource]], BuiltModule],
    ) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        write_config_yaml(yaml_file, both_cards_config)
        resource = create_built_resource(yaml_file, yaml_file)
        module = create_module(tmp_path, [resource])
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.side_effect = ToolkitAPIError("Server error", code=500)
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        with pytest.raises(ToolkitAPIError, match="Server error"):
            list(rule.validate())
