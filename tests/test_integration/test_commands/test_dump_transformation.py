from pathlib import Path

import pytest
from cognite.client.data_classes import (
    Transformation,
    TransformationDestination,
    TransformationNotification,
    TransformationNotificationWrite,
    TransformationSchedule,
    TransformationScheduleWrite,
    TransformationWrite,
)
from cognite.client.data_classes.transformations import NonceCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DumpResourceCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import TransformationFinder
from cognite_toolkit._cdf_tk.cruds import (
    TransformationCRUD,
    TransformationNotificationCRUD,
    TransformationScheduleCRUD,
)


@pytest.fixture(scope="session")
def deployed_transformation(toolkit_client: ToolkitClient) -> Transformation:
    transformation = TransformationWrite(
        external_id="toolkit_test_transformation",
        name="Toolkit Test Transformation",
        ignore_null_fields=True,
        query="SELECT * FROM _cdf.assets",
        destination=TransformationDestination.assets(),
        conflict_mode="abort",
        is_public=True,
    )
    existing = toolkit_client.transformations.retrieve(external_id=transformation.external_id)
    if existing:
        return existing
    destination_session = toolkit_client.iam.sessions.create()
    source_session = toolkit_client.iam.sessions.create()
    transformation.destination_nonce = NonceCredentials(
        destination_session.id, destination_session.nonce, toolkit_client.config.project
    )
    transformation.source_nonce = NonceCredentials(
        source_session.id, source_session.nonce, toolkit_client.config.project
    )
    return toolkit_client.transformations.create(transformation)


@pytest.fixture(scope="session")
def deployed_transformation_schedule(toolkit_client: ToolkitClient, deployed_transformation: Transformation) -> None:
    schedule = TransformationScheduleWrite(
        interval="0 12 * * 1",
        external_id=deployed_transformation.external_id,
        is_paused=True,
    )
    existing = toolkit_client.transformations.schedules.retrieve(external_id=schedule.external_id)
    if existing:
        return existing
    return toolkit_client.transformations.schedules.create(schedule)


@pytest.fixture(scope="session")
def deployed_transformation_notification(
    toolkit_client: ToolkitClient, deployed_transformation: Transformation
) -> None:
    notification = TransformationNotificationWrite(
        destination="my@example.com",
        transformation_external_id=deployed_transformation.external_id,
    )

    existing = toolkit_client.transformations.notifications.list(
        transformation_external_id=deployed_transformation.external_id, destination=notification.destination, limit=1
    )
    if existing:
        return existing[0]
    return toolkit_client.transformations.notifications.create(notification)


class TestDumpTransformation:
    def test_dump_transformation_with_schedule_and_notification(
        self,
        toolkit_client: ToolkitClient,
        deployed_transformation: Transformation,
        deployed_transformation_schedule: TransformationSchedule,
        deployed_transformation_notification: TransformationNotification,
        tmp_path: Path,
    ) -> None:
        cmd = DumpResourceCommand(silent=True)
        cmd.dump_to_yamls(
            TransformationFinder(toolkit_client, (deployed_transformation.external_id,)),
            output_dir=tmp_path,
            clean=False,
            verbose=False,
        )

        transformation_folder = tmp_path / "transformations"
        assert transformation_folder.exists()
        assert sum(1 for _ in transformation_folder.glob(f"*{TransformationCRUD.kind}.yaml")) == 1
        assert sum(1 for _ in transformation_folder.glob(f"*{TransformationScheduleCRUD.kind}.yaml")) == 1
        assert sum(1 for _ in transformation_folder.glob(f"*{TransformationNotificationCRUD.kind}.yaml")) == 1
