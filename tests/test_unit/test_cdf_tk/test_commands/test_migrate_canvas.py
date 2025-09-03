from datetime import datetime, timezone

import pytest
from cognite.client.data_classes.data_modeling import NodeList, ViewId

from cognite_toolkit._cdf_tk.client.data_classes.canvas import Canvas, IndustrialCanvas, IndustrialCanvasApply
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import MigrationCanvasCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, ToolkitWarning


@pytest.fixture(scope="session")
def asset_centric_canvas() -> tuple[IndustrialCanvas, NodeList[InstanceSource]]:
    canvas = IndustrialCanvas.load(
        {
            "annotations": [],
            "canvas": [
                {
                    "createdTime": 1751540227230,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540558717,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "Canvas/v7": {
                                "createdBy": "aGQ-cBXUBY6bmmxqIdkFoA",
                                "isArchived": None,
                                "isLocked": None,
                                "name": "Asset-centric1",
                                "solutionTags": None,
                                "sourceCanvasId": None,
                                "updatedAt": "2025-07-03T11:02:37.733+00:00",
                                "updatedBy": "aGQ-cBXUBY6bmmxqIdkFoA",
                                "visibility": "public",
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 14,
                }
            ],
            "containerReferences": [
                {
                    "createdTime": 1751540264906,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265_cf372b29-3012-49ff-8daf-5043404c23d7",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540264906,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "ContainerReference/v2": {
                                "chartsId": None,
                                "containerReferenceType": "asset",
                                "height": 357,
                                "id": "cf372b29-3012-49ff-8daf-5043404c23d7",
                                "label": "Kelmarsh 6",
                                "maxHeight": None,
                                "maxWidth": None,
                                "resourceId": 3840956528416998,
                                "resourceSubId": None,
                                "width": 600,
                                "x": 0,
                                "y": 0,
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 1,
                },
                {
                    "createdTime": 1751540275336,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265_09d58ddf-bebb-4e4d-96db-1702da76a016",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540275336,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "ContainerReference/v2": {
                                "chartsId": None,
                                "containerReferenceType": "timeseries",
                                "height": 400,
                                "id": "09d58ddf-bebb-4e4d-96db-1702da76a016",
                                "label": "Hub temperature, standard deviation (Â°C)",
                                "maxHeight": None,
                                "maxWidth": None,
                                "resourceId": 11978459264156,
                                "resourceSubId": None,
                                "width": 700,
                                "x": 700,
                                "y": 0,
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 1,
                },
                {
                    "createdTime": 1751540544349,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265_5e2bf845-103c-4c17-8549-9f20329b7f98",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540558717,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "ContainerReference/v2": {
                                "chartsId": None,
                                "containerReferenceType": "event",
                                "height": 500,
                                "id": "5e2bf845-103c-4c17-8549-9f20329b7f98",
                                "label": "b18cdf8e-6568-4e2a-a267-535eb52f41bf",
                                "maxHeight": None,
                                "maxWidth": None,
                                "resourceId": 9004025980300864,
                                "resourceSubId": None,
                                "width": 600,
                                "x": -10,
                                "y": 418,
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 4,
                },
            ],
        }
    )
    mapping = NodeList[InstanceSource](
        [
            InstanceSource(
                space="MyNewInstanceSpace",
                external_id="my_asset",
                version=1,
                last_updated_time=1,
                created_time=1,
                resource_type="asset",
                id_=3840956528416998,
                preferred_consumer_view_id=ViewId("my_space", "DoctrinoAsset", "v1"),
            ),
            InstanceSource(
                space="MyNewInstanceSpace",
                external_id="my_timeseries",
                version=1,
                last_updated_time=1,
                created_time=1,
                resource_type="timeseries",
                id_=11978459264156,
                preferred_consumer_view_id=ViewId("my_space", "DoctrinoTimeSeries", "v1"),
            ),
            InstanceSource(
                space="MyNewInstanceSpace",
                external_id="my_event",
                version=1,
                last_updated_time=1,
                created_time=1,
                resource_type="event",
                id_=9004025980300864,
            ),
        ]
    )
    return canvas, mapping


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
