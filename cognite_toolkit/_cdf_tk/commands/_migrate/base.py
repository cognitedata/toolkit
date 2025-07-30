from abc import ABC, abstractmethod

from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    SpaceIDScope,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ToolkitMigrationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .data_model import INSTANCE_SOURCE_VIEW_ID


class BaseMigrateCommand(ToolkitCommand, ABC):
    @property
    @abstractmethod
    def schema_spaces(self) -> list[str]:
        """Return the schema spaces used by this migration command."""
        raise NotImplementedError()

    def source_acl(self, data_set_id: list[int]) -> Capability | None:
        """Return the source ACL for the given data set IDs."""
        # This method should be implemented in subclasses that needs access to a specific source ACL.
        # such as TimeSeries, Files, Assets, and so on.
        return None

    def validate_access(
        self, client: ToolkitClient, instance_spaces: list[str], data_set_ids: list[int] | None = None
    ) -> None:
        required_capabilities: list[Capability] = [
            DataModelsAcl(actions=[DataModelsAcl.Action.Read], scope=SpaceIDScope(self.schema_spaces)),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Read,
                    DataModelInstancesAcl.Action.Write,
                    DataModelInstancesAcl.Action.Write_Properties,
                ],
                scope=SpaceIDScope(instance_spaces),
            ),
        ]
        if data_set_ids is not None:
            source_acl = self.source_acl(data_set_ids)
            if source_acl is None:
                raise ValueError(
                    "Bug in Toolkit: the source ACL is not defined for this migration command. "
                    "Please implement the source_acl method."
                )
            required_capabilities.append(source_acl)
        if missing := client.iam.verify_capabilities(required_capabilities):
            raise AuthenticationError(f"Missing required capabilities: {humanize_collection(missing)}.", missing)

    @staticmethod
    def validate_instance_source_exists(client: ToolkitClient) -> None:
        view = client.data_modeling.views.retrieve(INSTANCE_SOURCE_VIEW_ID)
        if not view:
            raise ToolkitMigrationError(
                f"The migration mapping view {INSTANCE_SOURCE_VIEW_ID} does not exist. "
                f"Please run the `cdf migrate prepare` command to deploy the migration data model."
            )

    def validate_available_capacity(self, client: ToolkitClient, instance_count: int) -> None:
        """Validate that the project has enough capacity to accommodate the migration."""

        stats = client.data_modeling.statistics.project()

        available_capacity = stats.instances.instances_limit - stats.instances.instances
        available_capacity_after = available_capacity - instance_count

        if available_capacity_after < DMS_INSTANCE_LIMIT_MARGIN:
            raise ToolkitValueError(
                "Cannot proceed with migration, not enough instance capacity available. Total capacity after migration"
                f" would be {available_capacity_after:,} instances, which is less than the required margin of"
                f" {DMS_INSTANCE_LIMIT_MARGIN:,} instances. Please increase the instance capacity in your CDF project"
                f" or delete some existing instances before proceeding with the migration of {instance_count:,} assets."
            )
        total_instances = stats.instances.instances + instance_count
        self.console(
            f"Project has enough capacity for migration. Total instances after migration: {total_instances:,}."
        )
