from collections.abc import Callable, Iterable
from pathlib import Path

from cognite.client.data_classes import Asset, Label, LabelDefinition
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataSetScope,
    SpaceIDScope,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId
from cognite.client.exceptions import CogniteAPIError, CogniteException
from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._constants import DATA_MODELING_MAX_WRITE_WORKERS
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ResourceCreationError,
    ResourceRetrievalError,
    ToolkitMigrationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor

from .data_classes import MigrationMapping, MigrationMappingList
from .data_model import MAPPING_VIEW_ID


class MigrateAssetsCommand(ToolkitCommand):
    cdf_cdm = "cdf_cdm"
    asset_id = ViewId(cdf_cdm, "CogniteAsset", "v1")

    # This is the number of timeseries that can be written in parallel.
    chunk_size = 1000 * DATA_MODELING_MAX_WRITE_WORKERS

    def migrate_assets(
        self,
        client: ToolkitClient,
        mapping_file: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        mappings = MigrationMappingList.read_mapping_file(mapping_file)
        self._validate_access(client, mappings)
        self._validate_migration_mappings_exists(client)
        self._validate_available_capacity(client, mappings)
        iteration_count = len(mappings) // self.chunk_size + 1
        executor = ProducerWorkerExecutor[list[tuple[Asset, MigrationMapping]], list[NodeApply]](
            download_iterable=self._download_assets(client, mappings),
            process=self._as_cognite_assets,
            write=self._upload_assets(client, dry_run=dry_run, verbose=verbose),
            iteration_count=iteration_count,
            max_queue_size=10,
            download_description="Downloading assets",
            process_description="Converting assets to CogniteAssets",
            write_description="Uploading CogniteAssets",
        )
        executor.run()
        if executor.error_occurred:
            raise ResourceCreationError(executor.error_message)

        prefix = "Would have" if dry_run else "Successfully"
        self.console(f"{prefix} migrated {executor.total_items:,} assets to CogniteAssets.")

    def _validate_access(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        required_capabilities: list[Capability] = [
            DataModelsAcl(
                actions=[DataModelsAcl.Action.Read], scope=SpaceIDScope([self.cdf_cdm, MAPPING_VIEW_ID.space])
            ),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Read,
                    DataModelInstancesAcl.Action.Write,
                    DataModelInstancesAcl.Action.Write_Properties,
                ],
                scope=SpaceIDScope(list(mappings.spaces())),
            ),
        ]
        if data_set_ids := mappings.get_data_set_ids():
            required_capabilities.append(
                TimeSeriesAcl(
                    actions=[TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
                    scope=DataSetScope(list(data_set_ids)),
                )
            )
        if missing := client.iam.verify_capabilities(required_capabilities):
            raise AuthenticationError(f"Missing required capabilities: {humanize_collection(missing)}.")

    def _validate_migration_mappings_exists(self, client: ToolkitClient) -> None:
        view = client.data_modeling.views.retrieve(MAPPING_VIEW_ID)
        if not view:
            raise ToolkitMigrationError(
                f"The migration mapping view {MAPPING_VIEW_ID} does not exist. "
                f"Please run the `cdf migrate prepare` command to deploy the migration data model."
            )

    def _validate_available_capacity(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        """Validate that the project has enough capacity to accommodate the migration."""
        try:
            stats = client.data_modeling.statistics.project()
        except CogniteAPIError:
            # This endpoint is not yet in alpha, it may change or not be available.
            self.warn(HighSeverityWarning("Cannot check the instances capacity proceeding with migration anyway."))
            return
        available_capacity = stats.instances.instances_limit - stats.instances.instances
        available_capacity_after = available_capacity - len(mappings)

        if available_capacity_after < DMS_INSTANCE_LIMIT_MARGIN:
            raise ToolkitValueError(
                "Cannot proceed with migration, not enough instance capacity available. Total capacity after migration"
                f"would be {available_capacity_after:,} instances, which is less than the required margin of"
                f"{DMS_INSTANCE_LIMIT_MARGIN:,} instances. Please increase the instance capacity in your CDF project"
                f" or delete some existing instances before proceeding with the migration of {len(mappings):,} assets."
            )
        total_instances = stats.instances.instances + len(mappings)
        self.console(
            f"Project has enough capacity for migration. Total instances after migration: {total_instances:,}."
        )

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
                    source=MAPPING_VIEW_ID,
                    properties={
                        "resourceType": "asset",
                        "id": asset.id,
                        "dataSetId": asset.data_set_id,
                        "classicExternalId": asset.external_id,
                    },
                ),
            ],
        )
