from collections.abc import Callable
from functools import partial
from pathlib import Path

from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import NodeId
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import ViewSource
from cognite_toolkit._cdf_tk.commands._migrate.base import BaseMigrateCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping, MigrationMappingList
from cognite_toolkit._cdf_tk.utils.batch_processor import BatchResult, HTTPBatchProcessor
from cognite_toolkit._cdf_tk.utils.fileio import Chunk, CSVWriter, SchemaColumn, Uncompressed
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .data_model import SPACE


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
        # We use the migration space for both schema and mapping instances.
        schema_spaces = list(mappings.get_schema_spaces() | {SPACE.space})
        instance_spaces = list(mappings.get_instance_spaces() | {SPACE.space})
        data_set_ids = list(mappings.get_data_set_ids()) or None
        self.validate_access(
            client, instance_spaces=instance_spaces, schema_spaces=schema_spaces, data_set_ids=data_set_ids
        )
        view_mappings = client.migration.view_source.retrieve([mappings.get_mappings()])

        target_views = {mapping.view_id for mapping in view_mappings}
        self.validate_access(client, schema_spaces=list(target_views), instance_spaces=None)
        view_mapping_by_id = {mapping.external_id: mapping for mapping in view_mappings}

        self.validate_available_capacity(client, len(mappings))

        iteration_count = len(mappings) // chunk_size + 1
        console = Console()
        with (
            CSVWriter(
                Path.cwd(),
                kind=mappings.display_name.capitalize(),
                compression=Uncompressed,
                columns=self._csv_schema(),
            ) as writer,
            HTTPBatchProcessor[NodeId](
                endpoint_url=client.config.create_api_url("/models/instances"),
                config=client.config,
                as_id=lambda node: NodeId.load(node),  # type: ignore[arg-type]
                body_parameters={"autoCreateDirectRelations": True},
                method="POST",
                max_workers=2,
                batch_size=1000,
                max_retries=10,
                console=console,
                result_processor=self._write_results_to_csv(writer, verbose, mappings.display_name),
            ) as http_client,
        ):
            executor = ProducerWorkerExecutor[list[tuple[CogniteResource, MigrationMapping]], list[dict[str, JsonVal]]](
                download_iterable=mappings.download_iterable(client, chunk_size),
                process=partial(self.convert, view_mapping_by_id=view_mapping_by_id),
                write=self._upload_nodes(http_client, dry_run=dry_run, verbose=verbose, console=console),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description=f"Downloading {mappings.display_name} resources",
                process_description=f"Converting {mappings.display_name} to data modeling instances",
                write_description="Uploading data modeling instances",
                console=console,
            )
        executor.run()
        executor.raise_on_error()

        prefix = "Would have" if dry_run else "Successfully"
        self.console(f"{prefix} migrated {executor.total_items:,} {mappings.display_name} to data modeling instances.")

    @staticmethod
    def convert(
        items: list[tuple[CogniteResource, MigrationMapping]], view_mapping_by_id: dict[str, ViewSource]
    ) -> list[dict[str, JsonVal]]:
        """Convert CogniteResource and MigrationMapping to NodeApply instances."""
        raise NotImplementedError()

    @staticmethod
    def _upload_nodes(
        http_client: HTTPBatchProcessor,
        dry_run: bool,
        verbose: bool,
        console: Console,
    ) -> Callable[[list[dict[str, JsonVal]]], None]:
        if dry_run:

            def _no_op(items: list[dict[str, JsonVal]]) -> None:
                if verbose:
                    console.print(f"Would have uploaded {len(items):,} data modeling instances.")

            return _no_op
        return http_client.add_items

    @staticmethod
    def _csv_schema() -> list[SchemaColumn]:
        return [
            SchemaColumn("ResourceType", "string"),
            SchemaColumn("NodeSpace", "string"),
            SchemaColumn("NodeExternalId", "string"),
            SchemaColumn("ResponseStatus", "integer"),
            SchemaColumn("ResponseMessage", "string"),
        ]

    @staticmethod
    def _write_results_to_csv(
        writer: CSVWriter, verbose: bool, resource_type: str
    ) -> Callable[[BatchResult[NodeId]], None]:
        """Write results to CSV file."""

        def _write_results(batch: BatchResult[NodeId]) -> None:
            rows: list[Chunk] = (
                [  # type: ignore[assignment]
                    {
                        "ResourceType": resource_type,
                        "NodeSpace": item.item.space,
                        "NodeExternalId": item.item.external_id,
                        "ResponseStatus": item.status_code,
                        "ResponseMessage": item.message or "",
                    }
                    for item in batch.successful_items
                ]
                + [
                    {
                        "ResourceType": resource_type,
                        "NodeSpace": item.item.space,
                        "NodeExternalId": item.item.external_id,
                        "ResponseStatus": item.status_code,
                        "ResponseMessage": item.error_message,
                    }
                    for item in batch.failed_items
                ]
                + [
                    {
                        "ResourceType": resource_type,
                        "NodeSpace": "<UNKNOWN>",
                        "NodeExternalId": item.item,
                        "ResponseStatus": item.status_code,
                        "ResponseMessage": item.error_message,
                    }
                    for item in batch.unknown_ids
                ]
            )

            writer.write_chunks(rows)
            if verbose and batch.total_items:
                print(f"Wrote {batch.total_items:,} results to CSV file")

        return _write_results
