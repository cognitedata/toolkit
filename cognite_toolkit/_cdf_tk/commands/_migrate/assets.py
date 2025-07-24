from collections.abc import Callable, Iterable
from pathlib import Path

from cognite.client.data_classes import Asset, Label, LabelDefinition
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataSetScope,
)
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId, NodeId
from cognite.client.exceptions import CogniteException
from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._constants import DATA_MODELING_MAX_WRITE_WORKERS
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceCreationError,
    ResourceRetrievalError,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.batch_processor import HTTPBatchProcessor, ProcessorResult
from rich.console import Console
from .base import BaseMigrateCommand
from .data_classes import MigrationMapping, MigrationMappingList
from .data_model import INSTANCE_SOURCE_VIEW_ID


class MigrateAssetsCommand(BaseMigrateCommand):
    cdf_cdm = "cdf_cdm"
    asset_id = ViewId(cdf_cdm, "CogniteAsset", "v1")

    # This is the number of timeseries that can be written in parallel.
    chunk_size = 1000 * DATA_MODELING_MAX_WRITE_WORKERS

    def __inti__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning=print_warning, skip_tracking=skip_tracking, silent=silent)
        self._results: ProcessorResult[NodeId] = ProcessorResult()

    @property
    def schema_spaces(self) -> list[str]:
        return [f"{self.cdf_cdm}", INSTANCE_SOURCE_VIEW_ID.space]

    def source_acl(self, data_set_id: list[int]) -> Capability:
        return AssetsAcl(actions=[AssetsAcl.Action.Read], scope=DataSetScope(data_set_id))

    def migrate_assets(
        self,
        client: ToolkitClient,
        mapping_file: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        mappings = MigrationMappingList.read_mapping_file(mapping_file)
        self.validate_access(client, list(mappings.spaces()), list(mappings.get_data_set_ids()))
        self.validate_instance_source_exists(client)
        self.validate_available_capacity(client, len(mappings))
        console = Console()
        processor = HTTPBatchProcessor[NodeId](
            endpoint_url="", # Todo: Set the correct endpoint URL for the migration
            config=client.config,
            as_id=lambda node: node.as_id(),
            body_parameters={}, #Todo: Set the correct body parameters for the migration
            console=console,
        )
        with HTTPBatchProcessor() as processor:
            processor.start()
            iteration_count = len(mappings) // self.chunk_size + 1
            executor = ProducerWorkerExecutor[list[tuple[Asset, MigrationMapping]], list[tuple[NodeApply, ConversionReport]]](
                download_iterable=self._download_assets(client, mappings),
                process=self._as_cognite_assets,
                write=processor.add_items if not dry_run else self._no_op,
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description="Downloading assets",
                process_description="Converting assets to CogniteAssets",
                write_description="Uploading CogniteAssets",
                console=console,
            )
            executor.run()
            if executor.error_occurred:
                raise ResourceCreationError(executor.error_message)
            results = processor.results()

        prefix = "Would have" if dry_run else "Successfully"
        self.console(f"{prefix} migrated {executor.total_items:,} assets to CogniteAssets.")

    def _download_assets(
        self, client: ToolkitClient, mappings: MigrationMappingList
    ) -> Iterable[list[tuple[Asset, MigrationMapping]]]:
        for chunk in chunker_sequence(mappings, self.chunk_size):
            try:
                asset_list = client.assets.retrieve_multiple(
                    chunk.get_ids(), chunk.get_external_ids(), ignore_unknown_ids=True
                )
            except CogniteException as e:
                raise ResourceRetrievalError(f"Failed to retrieve {len(chunk):,} assets: {e!s}") from e
            mapping_by_id = chunk.as_mapping_by_id()
            chunk_list: list[tuple[Asset, MigrationMapping]] = []
            for asset in asset_list:
                if asset.id in mapping_by_id:
                    chunk_list.append((asset, mapping_by_id[asset.id]))
                elif asset.external_id in mapping_by_id:
                    chunk_list.append((asset, mapping_by_id[asset.external_id]))
            yield chunk_list

    def _as_cognite_assets(self, assets: list[tuple[Asset, MigrationMapping]]) -> list[NodeApply]:
        """Convert Asset objects to CogniteAssetApply objects."""
        return [self.as_cognite_asset(asset, mapping) for asset, mapping in assets]

    def upload_items(self, processor: HTTPBatchProcessor[NodeId], dry_run: bool, verbose: bool) -> Callable[[list[NodeApply]], None]:
        class ResourceIterator:
            ...
        iterator = ResourceIterator()
        processor.process(iterator)

        def upload_items(items: list[NodeApply]) -> None:
            if dry_run:
                if verbose:
                    print(f"Would have created {len(items):,} CogniteAssets.")
                return
            iterator.append(items)

        return upload_items


    @classmethod
    def _upload_assets(cls, client: ToolkitClient, dry_run: bool, verbose: bool) -> Callable[[list[NodeApply]], None]:
        def upload_assets(assets: list[NodeApply]) -> None:
            if dry_run:
                if verbose:
                    print(f"Would have created {len(assets):,} CogniteAssets.")
                return
            try:
                created = client.data_modeling.instances.apply_fast(assets)
            except CogniteException as e:
                raise ResourceCreationError(f"Failed to upsert CogniteAssets {len(assets):,}: {e!s}") from e
            if verbose:
                print(f"Created {len(created):,} CogniteAssets.")

        return upload_assets

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
