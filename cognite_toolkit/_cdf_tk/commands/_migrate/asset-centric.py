from functools import partial

from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import NodeApply

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import ViewSource
from cognite_toolkit._cdf_tk.commands._migrate.base import BaseMigrateCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping, MigrationMappingList
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor


class MigrateAssetCentricCommand(BaseMigrateCommand):
    def migrate_resource(
        self,
        client: ToolkitClient,
        mappings: MigrationMappingList,
        dry_run: bool = False,
        chunk_size: int = 1000,
        verbose: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        self.validate_migration_model_available(client)
        schema_spaces = mappings.get_schema_spaces()
        instance_spaces = mappings.spaces()
        data_set_ids = mappings.get_data_set_ids()
        self.validate_access(
            client, instance_spaces=instance_spaces, data_set_ids=data_set_ids, schema_spaces=schema_spaces
        )

        view_mappings = client.migration.view_source.retrieve([mappings.get_mappings()])
        view_mapping_by_id = {mapping.external_id: mapping for mapping in view_mappings}

        self.validate_available_capacity(client, len(mappings))

        iteration_count = len(mappings) // chunk_size + 1
        executor = ProducerWorkerExecutor(
            download_iterable=mappings.download_iterable(client),
            process=partial(self._convert, view_mapping_by_id=view_mapping_by_id),
            write=self._upload_nodes(client, dry_run=dry_run, verbose=verbose),
            iteration_count=iteration_count,
            max_queue_size=10,
            download_description=f"Downloading {mappings.display_name} resources",
            process_description=f"Converting {mappings.display_name} to data modeling instances",
            write_description="Uploading data modeling instances",
        )
        executor.run()
        if executor.error_occurred:
            raise ToolkitMigrationError(executor.error_message)

        prefix = "Would have" if dry_run else "Successfully"
        self.console(f"{prefix} migrated {executor.total_items:,} {mappings.display_name} to data modeling instances.")

    def _convert(
        self, items: list[tuple[CogniteResource, MigrationMapping]], view_mapping_by_id: dict[str, ViewSource]
    ) -> list[NodeApply]:
        """Convert Asset and MigrationMapping to NodeApply instances."""
        raise NotImplementedError()
