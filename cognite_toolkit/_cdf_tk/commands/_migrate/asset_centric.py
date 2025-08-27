from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Generic

from cognite.client.data_classes.data_modeling import MappedProperty, NodeApply, NodeId, NodeOrEdgeData, View, ViewId
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import ViewSource
from cognite_toolkit._cdf_tk.commands._migrate.base import BaseMigrateCommand, T_AssetCentricResource
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping, MigrationMappingList
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.batch_processor import BatchResult, HTTPBatchProcessor
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.fileio import Chunk, CSVWriter, SchemaColumn, Uncompressed, NDJsonWriter
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
import threading
from dataclasses import dataclass, field
from typing import  Dict, Optional

@dataclass
class Issue:
    identifier: str
    type: str
    message: str
    details: Optional[dict] = field(default_factory=dict)

class ThreadSafeIssueStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._issues: Dict[str, Issue] = {}

    def put(self, issue: Issue) -> None:
        with self._lock:
            self._issues[issue.identifier] = issue

    def update(self, identifier: str, **kwargs) -> None:
        with self._lock:
            if identifier in self._issues:
                for key, value in kwargs.items():
                    setattr(self._issues[identifier], key, value)

    def remove(self, identifier: str) -> None:
        with self._lock:
            self._issues.pop(identifier, None)

    def get(self, identifier: str) -> Optional[Issue]:
        with self._lock:
            return self._issues.get(identifier)

from .data_model import INSTANCE_SOURCE_VIEW_ID, SPACE


class MigrateAssetCentricCommand(BaseMigrateCommand, Generic[T_AssetCentricResource]):
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
        referenced_views = list({view.view_id for view in view_mappings})
        views = client.data_modeling.views.retrieve(referenced_views)
        if missing := set(referenced_views) - {view.id for view in views}:
            raise ToolkitValueError(
                f"The following view IDs are missing in Data Modeling: {humanize_collection(missing)}"
            )
        view_by_id = {view: view.as_id() for view in views}

        target_views = {mapping.view_id for mapping in view_mappings}
        self.validate_access(client, schema_spaces=list(target_views), instance_spaces=None)
        view_mapping_by_id = {mapping.external_id: mapping for mapping in view_mappings}

        self.validate_available_capacity(client, len(mappings))

        iteration_count = len(mappings) // chunk_size + 1
        console = Console()
        with (
            CSVWriter(
                Path.cwd(),
                kind=f"{mappings.display_name.capitalize()}Migration",
                compression=Uncompressed,
                columns=self._csv_schema(),
            ) as csv_writer,
            NDJsonWriter(
                output_dir=Path.cwd(),
                kind=f"{mappings.display_name.capitalize()}MigrationDetailed",
                compression=Uncompressed,
            ) as ndjson_writer,
            HTTPBatchProcessor[NodeId](
                endpoint_url=client.config.create_api_url("/models/instances"),
                config=client.config,
                as_id=NodeId.load,
                body_parameters={"autoCreateDirectRelations": True},
                method="POST",
                max_workers=2,
                batch_size=1000,
                max_retries=10,
                console=console,
                result_processor=self._write_results_to_file(csv_writer, ndjson_writer, verbose, mappings.display_name),
            ) as http_client,
        ):


            executor = ProducerWorkerExecutor[
                list[tuple[T_AssetCentricResource, MigrationMapping]], list[dict[str, JsonVal]]
            ](
                download_iterable=mappings.download_iterable(client, chunk_size),
                process=partial(self.convert, view_mapping_by_id=view_mapping_by_id, view_by_id=view_by_id),
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
        items: list[tuple[T_AssetCentricResource, MigrationMapping]],
        view_mapping_by_id: dict[str, ViewSource],
        view_by_id: dict[ViewId, View],
        resource_type: str,
    ) -> list[dict[str, JsonVal]]:
        """Convert CogniteResource and MigrationMapping to NodeApply instances."""
        results: list[dict[str, JsonVal]] = []
        for resource, mapping in items:
            try:
                view_source = view_mapping_by_id[mapping.mapping]
            except KeyError as e:
                # This should never happen, as we validate this before starting the migration.
                raise ValueError(f"Bug in Toolkit. View source with id '{mapping.mapping}' not found") from e
            try:
                view = view_by_id[view_source.view_id]
            except KeyError as e:
                # This should never happen, as we validate this before starting the migration.
                raise ValueError(f"Bug in Toolkit. View with id '{view_source.view_id}' not found") from e
            dumped = resource.dump()
            properties: dict[str, PropertyValueWrite] = {}
            for prop_id, dm_prop_id in view_source.mapping.to_property_id.items():
                if prop_id not in dumped:
                    # Todo: Log?
                    continue
                if dm_prop_id not in view.properties:
                    # Todo: Warning?
                    continue
                dm_prop = view.properties[dm_prop_id]
                if not isinstance(dm_prop, MappedProperty):
                    # Todo: Warning?
                    continue
                try:
                    value = asset_centric_convert_to_primary_property(
                        dumped[prop_id],
                        dm_prop.type,
                        dm_prop.nullable,
                        (dm_prop.container, dm_prop.container_property_identifier),
                        (resource_type, prop_id),
                    )
                except ValueError:
                    # Todo: Warning?
                    continue
                properties[dm_prop_id] = value
            metadata = resource.metadata or {}
            for key, dm_prop_id in view_source.mapping.metadata_to_property_id.items():
                if key not in metadata:
                    # Todo: Log?
                    continue
                if dm_prop_id not in view.properties:
                    # Todo: Warning?
                    continue
                dm_prop = view.properties[dm_prop_id]
                if not isinstance(dm_prop, MappedProperty):
                    # Todo: Warning?
                    continue
                try:
                    value = convert_to_primary_property(
                        metadata[key],
                        dm_prop.type,
                        dm_prop.nullable,
                    )
                except ValueError:
                    # Todo: Warning?
                    continue
                properties[dm_prop_id] = value

            node = NodeApply(
                space=mapping.instance_id.space,
                external_id=mapping.instance_id.external_id,
                sources=[
                    NodeOrEdgeData(
                        source=view_source.view_id,
                        properties=properties,
                    ),
                    NodeOrEdgeData(
                        source=INSTANCE_SOURCE_VIEW_ID,
                        properties={
                            "resourceType": resource_type,
                            "id": resource.id,
                            "dataSetId": resource.data_set_id,
                            "classicExternalId": resource.external_id,
                        },
                    ),
                ],
            ).dump()
            results.append(node)
        return results

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
            SchemaColumn("ResourceId", "integer"),
            SchemaColumn("InstanceSpace", "string"),
            SchemaColumn("InstanceExternalId", "string"),
            SchemaColumn("ReadStatus", "string"),
            SchemaColumn("ConvertStatus", "string"),
            SchemaColumn("UploadStatus", "string"),
            SchemaColumn("MigrationResult", "string"),
        ]

    @staticmethod
    def _write_results_to_file(
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
