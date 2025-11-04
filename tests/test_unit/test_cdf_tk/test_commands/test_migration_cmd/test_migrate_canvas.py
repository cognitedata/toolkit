from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from cognite.client._api.iam import IAMAPI, ComparableCapability
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    ProjectCapabilityList,
)
from cognite.client.data_classes.data_modeling import DataModel, DataModelList, NodeList

from cognite_toolkit._cdf_tk.client.data_classes.canvas import Canvas, IndustrialCanvas, IndustrialCanvasApply
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import MigrationCanvasCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_model import (
    COGNITE_MIGRATION_MODEL,
    INSTANCE_SOURCE_VIEW_ID,
    MODEL_ID,
    RESOURCE_VIEW_MAPPING_VIEW_ID,
)
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitMigrationError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, ToolkitWarning


class TestMigrationCanvasCommand:
    def test_migrate_canvas_happy_path(
        self, asset_centric_canvas: tuple[IndustrialCanvas, NodeList[InstanceSource]]
    ) -> None:
        command = MigrationCanvasCommand(silent=True)
        canvas, instance_sources = asset_centric_canvas

        with monkeypatch_toolkit_client() as client:
            client.iam.verify_capabilities.return_value = []
            client.data_modeling.data_models.retrieve.return_value = [COGNITE_MIGRATION_MODEL]
            client.canvas.industrial.retrieve.return_value = canvas
            client.migration.instance_source.retrieve.return_value = instance_sources

            command.migrate_canvas(client, external_ids=["my_canvas"], dry_run=False, verbose=True)

            client.canvas.industrial.update.assert_called_once()
            update = client.canvas.industrial.update.call_args[0][0]
            assert isinstance(update, IndustrialCanvasApply)
            client.canvas.industrial.create.assert_called_once()
            backup = client.canvas.industrial.create.call_args[0][0]
            assert isinstance(backup, IndustrialCanvasApply)

        assert len(update.fdm_instance_container_references) == len(canvas.container_references)
        assert len(backup.fdm_instance_container_references) == 0

    def test_migrate_canvas_missing(self) -> None:
        command = MigrationCanvasCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            client.iam.verify_capabilities.return_value = []
            client.data_modeling.data_models.retrieve.return_value = [COGNITE_MIGRATION_MODEL]
            client.canvas.industrial.retrieve.return_value = None
            command.migrate_canvas(client, external_ids=["non-existing"], dry_run=False, verbose=True)
        assert len(command.warning_list) == 1
        warning = command.warning_list[0]
        assert isinstance(warning, ToolkitWarning)
        assert "Canvas with external ID 'non-existing' not found." in str(warning)

    def test_migrate_canvas_missing_instance_source(
        self, asset_centric_canvas: tuple[IndustrialCanvas, NodeList[InstanceSource]]
    ) -> None:
        command = MigrationCanvasCommand(silent=True)
        canvas, _ = asset_centric_canvas

        with monkeypatch_toolkit_client() as client:
            client.iam.verify_capabilities.return_value = []
            client.data_modeling.data_models.retrieve.return_value = [COGNITE_MIGRATION_MODEL]
            client.canvas.industrial.retrieve.return_value = canvas
            client.migration.instance_source.retrieve.return_value = NodeList[InstanceSource]([])

            command.migrate_canvas(client, external_ids=[canvas.canvas.external_id], dry_run=False, verbose=True)

        assert len(command.warning_list) == 1
        warning = command.warning_list[0]
        assert isinstance(warning, HighSeverityWarning)
        assert "Canvas 'Asset-centric1' has references to resources that are not been migrated" in str(warning)

    def test_migrate_canvas_no_asset_centric_references(
        self, asset_centric_canvas: tuple[IndustrialCanvas, NodeList[InstanceSource]]
    ) -> None:
        command = MigrationCanvasCommand(silent=True)
        canvas = IndustrialCanvas(
            canvas=Canvas(
                "canvasSpace",
                "canvasExternalId",
                1,
                1,
                1,
                name="MyCanvas",
                created_by="me",
                updated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                updated_by="me",
            )
        )

        with monkeypatch_toolkit_client() as client:
            client.iam.verify_capabilities.return_value = []
            client.data_modeling.data_models.retrieve.return_value = [COGNITE_MIGRATION_MODEL]
            client.canvas.industrial.retrieve.return_value = canvas

            command.migrate_canvas(client, external_ids=[canvas.canvas.external_id], dry_run=False, verbose=True)

        assert len(command.warning_list) == 1
        warning = command.warning_list[0]
        assert isinstance(warning, ToolkitWarning)
        assert "Canvas with name 'MyCanvas' does not have any asset-centric references." in str(warning)

    def test_validate_access(self) -> None:
        expected_missing = [
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Read,
                ],
                scope=DataModelInstancesAcl.Scope.SpaceID(["my_instance_space"]),
            ),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Write,
                ],
                scope=DataModelInstancesAcl.Scope.SpaceID(["my_instance_space"]),
            ),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Write_Properties,
                ],
                scope=DataModelInstancesAcl.Scope.SpaceID(["my_instance_space"]),
            ),
            DataModelsAcl(actions=[DataModelsAcl.Action.Read], scope=DataModelsAcl.Scope.SpaceID(["dummy_space"])),
        ]

        with monkeypatch_toolkit_client() as client:
            existing_capabilities = ProjectCapabilityList([], cognite_client=client)

            def verify_capabilities(desired_capabilities: ComparableCapability) -> list[Capability]:
                return IAMAPI.compare_capabilities(
                    existing_capabilities,
                    desired_capabilities,
                )

            client.iam.verify_capabilities = verify_capabilities
            with pytest.raises(AuthenticationError) as exc_info:
                MigrationCanvasCommand.validate_access(client, ["my_instance_space"], ["dummy_space"])
        error = exc_info.value
        assert isinstance(error, AuthenticationError)
        assert "Missing required capabilities" in error.args[0]
        assert error.args[1] == expected_missing

    def test_validate_migration_model_available(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.data_models.retrieve.return_value = DataModelList([])
            with pytest.raises(ToolkitMigrationError):
                MigrationCanvasCommand.validate_migration_model_available(client)

    def test_validate_migration_model_available_multiple_models(self) -> None:
        """Test that multiple models raises an error."""
        with monkeypatch_toolkit_client() as client:
            # Create mock models with the expected MODEL_ID
            model1 = MagicMock(spec=DataModel)
            model1.as_id.return_value = MODEL_ID
            model2 = MagicMock(spec=DataModel)
            model2.as_id.return_value = MODEL_ID

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model1, model2])

            with pytest.raises(ToolkitMigrationError) as exc_info:
                MigrationCanvasCommand.validate_migration_model_available(client)

            assert "Multiple migration models" in str(exc_info.value)

    def test_validate_migration_model_available_missing_views(self) -> None:
        """Test that a model with missing views raises an error."""
        with monkeypatch_toolkit_client() as client:
            model = MagicMock(spec=DataModel)
            model.as_id.return_value = MODEL_ID
            # Model has views but missing the required ones
            model.views = [INSTANCE_SOURCE_VIEW_ID]  # Missing VIEW_SOURCE_VIEW_ID

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model])

            with pytest.raises(ToolkitMigrationError, match=r"Invalid migration model. Missing views"):
                MigrationCanvasCommand.validate_migration_model_available(client)

    def test_validate_migration_model_available_success(self) -> None:
        """Test that a valid model with all required views succeeds."""
        with monkeypatch_toolkit_client() as client:
            # Mocking the migration Model to get a response format of the model.
            # An alternative would be to write a conversion of write -> read format of the model
            # which is a significant amount of logic.
            model = MagicMock(spec=DataModel)
            model.as_id.return_value = MODEL_ID
            # Model has all required views
            model.views = [INSTANCE_SOURCE_VIEW_ID, RESOURCE_VIEW_MAPPING_VIEW_ID]

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model])

            # Should not raise any exception
            MigrationCanvasCommand.validate_migration_model_available(client)

            client.data_modeling.data_models.retrieve.assert_called_once_with([MODEL_ID], inline_views=False)
