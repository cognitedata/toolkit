from unittest.mock import MagicMock

import pytest
from cognite.client._api.iam import IAMAPI, ComparableCapability
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataSetScope,
    ProjectCapabilityList,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import DataModel, DataModelList
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, ProjectStatistics

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.base import BaseMigrateCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_model import (
    INSTANCE_SOURCE_VIEW_ID,
    MODEL_ID,
    RESOURCE_VIEW_MAPPING_VIEW_ID,
)
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitMigrationError, ToolkitValueError


class DummyMigrationCommand(BaseMigrateCommand):
    """A dummy command for testing purposes."""

    def source_acl(self, data_set_id: list[int]) -> TimeSeriesAcl:
        return TimeSeriesAcl(
            actions=[TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            scope=DataSetScope(data_set_id),
        )


class TestBaseCommand:
    def test_validate_access(self) -> None:
        cmd = DummyMigrationCommand(silent=True)
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
            TimeSeriesAcl(actions=[TimeSeriesAcl.Action.Read], scope=DataSetScope([123])),
            TimeSeriesAcl(actions=[TimeSeriesAcl.Action.Write], scope=DataSetScope([123])),
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
                cmd.validate_access(client, ["my_instance_space"], ["dummy_space"], [123])
        error = exc_info.value
        assert isinstance(error, AuthenticationError)
        assert "Missing required capabilities" in error.args[0]
        assert error.args[1] == expected_missing

    def test_validate_migration_model_available(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.data_models.retrieve.return_value = DataModelList([])
            with pytest.raises(ToolkitMigrationError):
                BaseMigrateCommand.validate_migration_model_available(client)

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
                BaseMigrateCommand.validate_migration_model_available(client)

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
                BaseMigrateCommand.validate_migration_model_available(client)

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
            BaseMigrateCommand.validate_migration_model_available(client)

            client.data_modeling.data_models.retrieve.assert_called_once_with([MODEL_ID], inline_views=False)

    def test_validate_available_capacity_missing_capacity(self) -> None:
        cmd = DummyMigrationCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            stats = MagicMock(spec=ProjectStatistics)
            stats.instances = InstanceStatistics(
                nodes=1000,
                edges=0,
                soft_deleted_edges=0,
                soft_deleted_nodes=0,
                instances_limit=1500,
                soft_deleted_instances_limit=10_000,
                instances=1000,
                soft_deleted_instances=0,
            )
            client.data_modeling.statistics.project.return_value = stats
            with pytest.raises(ToolkitValueError) as exc_info:
                cmd.validate_available_capacity(client, 10_000)

        assert "Cannot proceed with migration" in str(exc_info.value)

    def test_validate_available_capacity_sufficient_capacity(self) -> None:
        cmd = DummyMigrationCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            stats = MagicMock(spec=ProjectStatistics)
            stats.instances = InstanceStatistics(
                nodes=1000,
                edges=0,
                soft_deleted_edges=0,
                soft_deleted_nodes=0,
                instances_limit=5_000_000,
                soft_deleted_instances_limit=100_000_000,
                instances=1000,
                soft_deleted_instances=0,
            )
            client.data_modeling.statistics.project.return_value = stats
            cmd.validate_available_capacity(client, 10_000)
