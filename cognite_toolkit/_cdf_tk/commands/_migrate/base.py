from abc import ABC, abstractmethod

from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    SpaceIDScope,
)
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ToolkitMigrationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .data_model import MAPPING_VIEW_ID


class BaseMigrateCommand(ToolkitCommand, ABC):
    @abstractmethod
    @property
    def schema_spaces(self) -> list[str]:
        """Return the schema spaces used by this migration command."""
        raise NotImplementedError()

    @abstractmethod
    def source_acl(self, data_set_id: list[int]) -> Capability:
        raise NotImplementedError()

    def _validate_access(
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
            required_capabilities.append(self.source_acl(data_set_ids))
        if missing := client.iam.verify_capabilities(required_capabilities):
            raise AuthenticationError(f"Missing required capabilities: {humanize_collection(missing)}.")

    @staticmethod
    def _validate_instance_source_exists(client: ToolkitClient) -> None:
        view = client.data_modeling.views.retrieve(MAPPING_VIEW_ID)
        if not view:
            raise ToolkitMigrationError(
                f"The migration mapping view {MAPPING_VIEW_ID} does not exist. "
                f"Please run the `cdf migrate prepare` command to deploy the migration data model."
            )

    def _validate_available_capacity(self, client: ToolkitClient, instance_count: int) -> None:
        """Validate that the project has enough capacity to accommodate the migration."""
        try:
            stats = client.data_modeling.statistics.project()
        except CogniteAPIError:
            # This endpoint is not yet in alpha, it may change or not be available.
            self.warn(HighSeverityWarning("Cannot check the instances capacity proceeding with migration anyway."))
            return
        available_capacity = stats.instances.instances_limit - stats.instances.instances
        available_capacity_after = available_capacity - instance_count

        if available_capacity_after < DMS_INSTANCE_LIMIT_MARGIN:
            raise ToolkitValueError(
                "Cannot proceed with migration, not enough instance capacity available. Total capacity after migration"
                f"would be {available_capacity_after:,} instances, which is less than the required margin of"
                f"{DMS_INSTANCE_LIMIT_MARGIN:,} instances. Please increase the instance capacity in your CDF project"
                f" or delete some existing instances before proceeding with the migration of {instance_count:,} assets."
            )
        total_instances = stats.instances.instances + instance_count
        self.console(
            f"Project has enough capacity for migration. Total instances after migration: {total_instances:,}."
        )
