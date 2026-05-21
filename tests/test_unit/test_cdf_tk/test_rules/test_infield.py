from pathlib import Path
from unittest.mock import MagicMock

import yaml

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, FailedValidation
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId, ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import (
    AbsoluteDirPath,
    AbsoluteFilePath,
    RelativeDirPath,
)
from cognite_toolkit._cdf_tk.resource_ios import InFieldCDMLocationConfigIO
from cognite_toolkit._cdf_tk.rules._infield import InFieldCDMViewPropertiesRuleSet


class TestInFieldCDMViewPropertiesRuleSet:
    _ACTIVITIES_REQUIRED = frozenset({"sourceId", "name", "status", "type", "mainAsset"})
    _NOTIFICATIONS_REQUIRED = frozenset(
        {"sourceId", "type", "status", "description", "asset", "createdDate", "priority"}
    )

    @staticmethod
    def _write_config_yaml(filepath: Path, content: dict) -> None:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            yaml.safe_dump(content, f)

    @staticmethod
    def _create_built_resource(source_path: Path, build_path: Path) -> BuiltResource:
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

    @staticmethod
    def _create_module(tmp_path: Path, resources: list[BuiltResource]) -> BuiltModule:
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

    @staticmethod
    def _mock_view(view_id: ViewId, property_names: frozenset[str]) -> MagicMock:
        view = MagicMock()
        view.properties = {name: MagicMock() for name in property_names}
        view.as_id.return_value = view_id
        return view

    def _both_cards_config(self) -> dict:
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

    def test_get_status_with_client(self) -> None:
        rule = InFieldCDMViewPropertiesRuleSet(modules=[], client=MagicMock())
        status = rule.get_status()
        assert status.code == "ready"

    def test_get_status_without_client(self) -> None:
        rule = InFieldCDMViewPropertiesRuleSet(modules=[])
        status = rule.get_status()
        assert status.code == "reduced"

    def test_valid_view_has_all_required_properties(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        self._write_config_yaml(yaml_file, self._both_cards_config())
        resource = self._create_built_resource(yaml_file, yaml_file)
        module = self._create_module(tmp_path, [resource])

        activities_id = ViewId(space="customer_idm", external_id="ActivitiesCard", version="v2")
        notifications_id = ViewId(space="customer_idm", external_id="NotificationsCard", version="v2")

        mock_client = MagicMock()

        def retrieve(view_ids: list[ViewId], include_inherited_properties: bool = True) -> list[MagicMock]:
            views = []
            for view_id in view_ids:
                if view_id == activities_id:
                    views.append(self._mock_view(activities_id, self._ACTIVITIES_REQUIRED))
                elif view_id == notifications_id:
                    views.append(self._mock_view(notifications_id, self._NOTIFICATIONS_REQUIRED))
            return views

        mock_client.tool.views.retrieve.side_effect = retrieve
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        results = list(rule.validate())
        assert results == []
        mock_client.tool.views.retrieve.assert_called_once()

    def test_view_missing_required_properties(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        self._write_config_yaml(
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
        resource = self._create_built_resource(yaml_file, yaml_file)
        module = self._create_module(tmp_path, [resource])
        activities_id = ViewId(space="customer_idm", external_id="ActivitiesCard", version="v2")
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = [
            self._mock_view(activities_id, self._ACTIVITIES_REQUIRED - {"mainAsset"})
        ]
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        errors = [r for r in rule.validate() if isinstance(r, ConsistencyError)]
        assert len(errors) == 1
        assert errors[0].code == InFieldCDMViewPropertiesRuleSet.CODE
        assert "mainAsset" in errors[0].message

    def test_view_not_found_in_cdf(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        self._write_config_yaml(
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
        resource = self._create_built_resource(yaml_file, yaml_file)
        module = self._create_module(tmp_path, [resource])
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = []
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        errors = [r for r in rule.validate() if isinstance(r, ConsistencyError)]
        assert len(errors) == 1
        assert errors[0].code == InFieldCDMViewPropertiesRuleSet.CODE
        assert "was not found in CDF" in errors[0].message

    def test_retrieve_called_once_for_multiple_resources(self, tmp_path: Path) -> None:
        yaml_file_1 = tmp_path / "cdf_applications" / "loc1.InFieldCDMLocationConfig.yaml"
        yaml_file_2 = tmp_path / "cdf_applications" / "loc2.InFieldCDMLocationConfig.yaml"
        self._write_config_yaml(yaml_file_1, self._both_cards_config())
        self._write_config_yaml(yaml_file_2, self._both_cards_config())
        resources = [
            self._create_built_resource(yaml_file_1, yaml_file_1),
            self._create_built_resource(yaml_file_2, yaml_file_2),
        ]
        module = self._create_module(tmp_path, resources)

        activities_id = ViewId(space="customer_idm", external_id="ActivitiesCard", version="v2")
        notifications_id = ViewId(space="customer_idm", external_id="NotificationsCard", version="v2")
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = [
            self._mock_view(activities_id, self._ACTIVITIES_REQUIRED),
            self._mock_view(notifications_id, self._NOTIFICATIONS_REQUIRED),
        ]
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        list(rule.validate())
        mock_client.tool.views.retrieve.assert_called_once()
        call_view_ids = mock_client.tool.views.retrieve.call_args[0][0]
        assert set(call_view_ids) == {activities_id, notifications_id}

    def test_retrieve_batch_failure_yields_failed_validation(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "cdf_applications" / "my_location.InFieldCDMLocationConfig.yaml"
        self._write_config_yaml(yaml_file, self._both_cards_config())
        resource = self._create_built_resource(yaml_file, yaml_file)
        module = self._create_module(tmp_path, [resource])
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.side_effect = ToolkitAPIError("Server error", code=500)
        rule = InFieldCDMViewPropertiesRuleSet(modules=[module], client=mock_client)
        results = list(rule.validate())
        assert len(results) == 1
        assert isinstance(results[0], FailedValidation)
        assert results[0].source == "batch"
        assert "Failed to retrieve card views from CDF" in results[0].message
