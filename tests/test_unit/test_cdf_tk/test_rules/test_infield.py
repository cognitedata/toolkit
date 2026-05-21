from pathlib import Path
from unittest.mock import MagicMock

import yaml

from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
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
    def _mock_view(property_names: frozenset[str]) -> MagicMock:
        view = MagicMock()
        view.properties = {name: MagicMock() for name in property_names}
        return view

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
                    "assetNotificationsCard": {
                        "space": "customer_idm",
                        "version": "v2",
                        "externalId": "NotificationsCard",
                    },
                },
            },
        )
        resource = self._create_built_resource(yaml_file, yaml_file)
        mock_client = MagicMock()

        def retrieve(view_ids: list[ViewId], include_inherited_properties: bool = True) -> list[MagicMock]:
            view_id = view_ids[0]
            if view_id.external_id == "ActivitiesCard":
                return [self._mock_view(self._ACTIVITIES_REQUIRED)]
            if view_id.external_id == "NotificationsCard":
                return [self._mock_view(self._NOTIFICATIONS_REQUIRED)]
            return []

        mock_client.tool.views.retrieve.side_effect = retrieve
        rule = InFieldCDMViewPropertiesRuleSet(modules=[], client=mock_client)
        errors = list(rule._validate_config(resource))
        assert len(errors) == 0

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
        mock_client = MagicMock()
        mock_client.tool.views.retrieve.return_value = [self._mock_view(self._ACTIVITIES_REQUIRED - {"mainAsset"})]
        rule = InFieldCDMViewPropertiesRuleSet(modules=[], client=mock_client)
        errors = list(rule._validate_config(resource))
        assert len(errors) == 1
        assert isinstance(errors[0], ConsistencyError)
        assert errors[0].code == InFieldCDMViewPropertiesRuleSet.CODE
        assert "mainAsset" in errors[0].message
