from collections.abc import Callable, Iterable
from functools import partial
from pathlib import Path

from cognite.client.data_classes import Asset, Label, LabelDefinition
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataSetScope,
)
from cognite.client.data_classes.data_modeling import NodeApply, NodeId, NodeOrEdgeData, ViewId
from cognite.client.exceptions import CogniteException
from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceRetrievalError,
)
from cognite_toolkit._cdf_tk.utils.batch_processor import BatchResult, HTTPBatchProcessor
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.table_writers import CSVWriter, Rows, Schema, SchemaColumn, SchemaColumnList
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .base import BaseMigrateCommand
from .data_classes import MigrationMapping, MigrationMappingList
from .data_model import INSTANCE_SOURCE_VIEW_ID


class MigrateAssetsCommand(BaseMigrateCommand):
    cdf_cdm = "cdf_cdm"
    asset_id = ViewId(cdf_cdm, "CogniteAsset", "v1")

    chunk_size = 1000  # Number of assets to process in each batch

    def source_acl(self, data_set_id: list[int]) -> Capability:
        return AssetsAcl(actions=[AssetsAcl.Action.Read], scope=DataSetScope(data_set_id))

    def migrate_assets(
        self,
        client: ToolkitClient,
        mapping_file: Path,
        dry_run: bool = False,
        max_workers: int = 2,
        output_dir: Path | None = None,
        verbose: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        mappings = MigrationMappingList.read_mapping_file(mapping_file, "asset")
        self.validate_access(
            client,
            instance_spaces=list(mappings.spaces()),
            schema_spaces=[f"{self.cdf_cdm}", INSTANCE_SOURCE_VIEW_ID.space],
            data_set_ids=list(mappings.get_data_set_ids()),
        )
        self.validate_migration_model_available(client)
        self.validate_available_capacity(client, len(mappings))

        output_dir = output_dir or Path.cwd()
        console = Console()
        iteration_count = len(mappings) // self.chunk_size + (1 if len(mappings) % self.chunk_size > 0 else 0)
        with (
            CSVWriter(self._csv_schema(), output_dir=output_dir) as writer,
            HTTPBatchProcessor[NodeId](
                endpoint_url=client.config.create_api_url("/models/instances"),
                config=client.config,
                as_id=lambda node: NodeId.load(node),  # type: ignore[arg-type]
                result_processor=self._write_results_to_csv(writer, verbose),  # type: ignore[arg-type]
                method="POST",
                body_parameters={"autoCreateDirectRelations": True},
                console=console,
                max_workers=max_workers,
                batch_size=self.chunk_size,
            ) as processor,
        ):
            executor = ProducerWorkerExecutor[list[tuple[Asset, MigrationMapping]], list[dict[str, JsonVal]]](
                download_iterable=self._download_assets(client, mappings),
                process=self._as_cognite_assets,
                write=processor.add_items if not dry_run else partial(self._no_op, verbose=verbose),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description="Downloading assets",
                process_description="Converting assets to CogniteAssets",
                write_description="Uploading CogniteAssets",
                console=console,
            )
            executor.run()
            executor.raise_on_error()

        prefix = "Would have" if dry_run else "Successfully"
        self.console(f"{prefix} migrated {executor.total_items:,} assets to CogniteAssets.")

    def _download_assets(
        self, client: ToolkitClient, mappings: MigrationMappingList
    ) -> Iterable[list[tuple[Asset, MigrationMapping]]]:
        for chunk in chunker_sequence(mappings, self.chunk_size):
            try:
                asset_list = client.assets.retrieve_multiple(ids=chunk.get_ids(), ignore_unknown_ids=True)
            except CogniteException as e:
                raise ResourceRetrievalError(f"Failed to retrieve {len(chunk):,} assets: {e!s}") from e
            mapping_by_id = chunk.as_mapping_by_id()
            chunk_list: list[tuple[Asset, MigrationMapping]] = []
            for asset in asset_list:
                if asset.id in mapping_by_id:
                    chunk_list.append((asset, mapping_by_id[asset.id]))
            yield chunk_list

    def _as_cognite_assets(self, assets: list[tuple[Asset, MigrationMapping]]) -> list[dict[str, JsonVal]]:
        """Convert Asset objects to CogniteAssetApply objects."""
        return [self.as_cognite_asset(asset, mapping).dump(camel_case=True) for asset, mapping in assets]

    @classmethod
    def as_cognite_asset(cls, asset: Asset, mapping: MigrationMapping) -> NodeApply:
        tags: list[str] = []
        for label in asset.labels or []:
            if isinstance(label, str):
                tags.append(label)
            elif isinstance(label, dict) and "externalId" in label:
                tags.append(label["externalId"])
            elif isinstance(label, Label | LabelDefinition) and label.external_id:
                tags.append(label.external_id)

        return NodeApply(
            space=mapping.instance_id.space,
            external_id=mapping.instance_id.external_id,
            sources=[
                NodeOrEdgeData(
                    source=cls.asset_id,
                    properties={
                        "name": asset.name,
                        "description": asset.description,
                        "tags": tags or None,
                    },
                ),
                NodeOrEdgeData(
                    source=INSTANCE_SOURCE_VIEW_ID,
                    properties={
                        "resourceType": "asset",
                        "id": asset.id,
                        "dataSetId": asset.data_set_id,
                        "classicExternalId": asset.external_id,
                    },
                ),
            ],
        )

    @staticmethod
    def _no_op(items: Iterable[dict[str, JsonVal]], verbose: bool) -> None:
        """No operation function for dry runs."""
        if verbose:
            print(f"Would have written {len(list(items)):,} items")

    @staticmethod
    def _csv_schema() -> Schema:
        return Schema(
            "AssetMigration",
            folder_name="assets",
            kind="MigrationResults",
            format_="csv",
            columns=SchemaColumnList(
                [
                    SchemaColumn("ResourceType", "string"),
                    SchemaColumn("NodeSpace", "string"),
                    SchemaColumn("NodeExternalId", "string"),
                    SchemaColumn("ResponseStatus", "integer"),
                    SchemaColumn("ResponseMessage", "string"),
                ]
            ),
        )

    @staticmethod
    def _write_results_to_csv(writer: CSVWriter, verbose: bool) -> Callable[[BatchResult[NodeId]], None]:
        """Write results to CSV file."""

        def _write_results(batch: BatchResult[NodeId]) -> None:
            rows: Rows = (
                [  # type: ignore[assignment]
                    {
                        "ResourceType": "asset",
                        "NodeSpace": item.item.space,
                        "NodeExternalId": item.item.external_id,
                        "ResponseStatus": item.status_code,
                        "ResponseMessage": item.message or "",
                    }
                    for item in batch.successful_items
                ]
                + [
                    {
                        "ResourceType": "asset",
                        "NodeSpace": item.item.space,
                        "NodeExternalId": item.item.external_id,
                        "ResponseStatus": item.status_code,
                        "ResponseMessage": item.error_message,
                    }
                    for item in batch.failed_items
                ]
                + [
                    {
                        "ResourceType": "asset",
                        "NodeSpace": "<UNKNOWN>",
                        "NodeExternalId": item.item,
                        "ResponseStatus": item.status_code,
                        "ResponseMessage": item.error_message,
                    }
                    for item in batch.unknown_ids
                ]
            )

            writer.write_rows([("AssetMigration", rows)])
            if verbose and batch.total_items:
                print(f"Wrote {batch.total_items:,} results to CSV file")

        return _write_results
