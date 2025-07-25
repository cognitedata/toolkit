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
from cognite.client.data_classes.data_modeling import ViewList
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, ProjectStatistics

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.base import BaseMigrateCommand
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitMigrationError, ToolkitValueError


class DummyMigrationCommand(BaseMigrateCommand):
    """A dummy command for testing purposes."""

    @property
    def schema_spaces(self) -> list[str]:
        return ["dummy_space"]

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
            DataModelsAcl(actions=[DataModelsAcl.Action.Read], scope=DataModelsAcl.Scope.SpaceID(cmd.schema_spaces)),
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
                cmd.validate_access(client, ["my_instance_space"], [123])
        error = exc_info.value
        assert isinstance(error, AuthenticationError)
        assert "Missing required capabilities" in error.args[0]
        assert error.args[1] == expected_missing

    def test_validate_instance_source_exists_raise(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.views.retrieve.return_value = ViewList([])
            with pytest.raises(ToolkitMigrationError):
                BaseMigrateCommand.validate_instance_source_exists(client)

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
