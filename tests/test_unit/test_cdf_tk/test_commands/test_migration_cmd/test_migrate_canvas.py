from datetime import datetime, timezone

from cognite.client.data_classes.data_modeling import NodeList

from cognite_toolkit._cdf_tk.client.data_classes.canvas import Canvas, IndustrialCanvas, IndustrialCanvasApply
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import MigrationCanvasCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
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
