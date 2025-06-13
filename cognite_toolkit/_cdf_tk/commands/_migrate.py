import csv
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import SupportsIndex, overload

import questionary
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataSetScope,
    SpaceIDScope,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeId, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel
from rich.progress import track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ToolkitFileNotFoundError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from ._base import ToolkitCommand


@dataclass
class MigrationMapping:
    resource_type: str
    instance_id: NodeId


@dataclass
class IdMigrationMapping(MigrationMapping):
    id: int
    data_set_id: int | None = None


@dataclass
class ExternalIdMigrationMapping(MigrationMapping):
    external_id: str
    data_set_id: int | None = None


class MigrationMappingList(list, Sequence[MigrationMapping]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[MigrationMapping] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[MigrationMapping]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> MigrationMapping: ...

    @overload
    def __getitem__(self, index: slice) -> "MigrationMappingList": ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> "MigrationMapping | MigrationMappingList":
        if isinstance(index, slice):
            return MigrationMappingList(super().__getitem__(index))
        return super().__getitem__(index)

    def as_ids(self) -> list[int]:
        """Return a list of IDs from the migration mappings."""
        return [mapping.id for mapping in self if isinstance(mapping, IdMigrationMapping)]

    def as_external_ids(self) -> list[str]:
        """Return a list of external IDs from the migration mappings."""
        return [mapping.external_id for mapping in self if isinstance(mapping, ExternalIdMigrationMapping)]

    def as_node_ids(self) -> list[NodeId]:
        """Return a list of NodeIds from the migration mappings."""
        return [mapping.instance_id for mapping in self if isinstance(mapping, MigrationMapping)]

    def spaces(self) -> set[str]:
        """Return a set of spaces from the migration mappings."""
        return {mapping.instance_id.space for mapping in self}

    def as_pending_ids(self) -> list[PendingInstanceId]:
        return [
            PendingInstanceId(
                pending_instance_id=mapping.instance_id,
                id=mapping.id if isinstance(mapping, IdMigrationMapping) else None,
                external_id=mapping.external_id if isinstance(mapping, ExternalIdMigrationMapping) else None,
            )
            for mapping in self
        ]

    def as_data_set_ids(self) -> set[int]:
        """Return a list of data set IDs from the migration mappings."""
        return {
            mapping.data_set_id
            for mapping in self
            if isinstance(mapping, IdMigrationMapping | ExternalIdMigrationMapping) and mapping.data_set_id is not None
        }


class MigrateTimeseriesCommand(ToolkitCommand):
    cdf_cdm = "cdf_cdm"
    view_id = ViewId(cdf_cdm, "CogniteTimeSeries", "v1")
    cdf_cdm_units = "cdf_cdm_units"
    schema_spaces = frozenset({cdf_cdm, cdf_cdm_units})
    chunk_size = 1000

    def migrate_timeseries(
        self,
        client: ToolkitClient,
        mapping_file: Path,
        dry_run: bool = False,
        verbose: bool = False,
        auto_yes: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        mappings = self.read_mapping_file(mapping_file)

        self._validate_access(client, mappings)
        self._validate_timeseries_existence(client, mappings)
        self._validate_available_capacity(client, mappings)

        if dry_run:
            self.console(f"Dry run mode. Would have migrated {len(mappings):,} TimeSeries to CogniteTimeSeries.")
            return
        if not auto_yes and self._confirm(mappings) is False:
            return
        self._migrate(client, mappings, verbose)

    def _validate_access(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        required_capabilities: list[Capability] = [
            DataModelsAcl(actions=[DataModelsAcl.Action.Read], scope=SpaceIDScope(list(self.schema_spaces))),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Read,
                    DataModelInstancesAcl.Action.Write,
                    DataModelInstancesAcl.Action.Write_Properties,
                ],
                scope=SpaceIDScope(list(mappings.spaces())),
            ),
        ]
        if data_set_ids := mappings.as_data_set_ids():
            required_capabilities.append(
                TimeSeriesAcl(
                    actions=[TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
                    scope=DataSetScope(list(data_set_ids)),
                )
            )
        if missing := client.iam.verify_capabilities(required_capabilities):
            raise AuthenticationError(f"Missing required capabilities: {humanize_collection(missing)}.")

    def _validate_timeseries_existence(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        total_validated = 0
        chunk: MigrationMappingList
        for chunk in track(  # type: ignore[assignment]
            chunker_sequence(mappings, size=self.chunk_size),
            description="Validating...",
            total=len(mappings) // self.chunk_size + 1,
        ):
            try:
                timeseries = client.time_series.retrieve_multiple(
                    ids=chunk.as_ids(),
                    external_ids=chunk.as_external_ids(),
                    ignore_unknown_ids=True,
                )
            except CogniteAPIError as e:
                raise AuthenticationError(
                    f"Failed to retrieve TimeSeries. This is likely due to lack of permissions: {e!s}"
                ) from e

            missing_count = len(timeseries) - len(mappings)
            if missing_count > 0:
                raise ToolkitValueError(f"Missing {missing_count} TimeSeries does not exist in CDF.")

            existing_result = client.data_modeling.instances.retrieve(chunk.as_node_ids())
            if len(existing_result.nodes) != 0:
                raise ToolkitValueError(
                    "Some of the TimeSeries you are trying to migrate already exist in Data Modeling. "
                    f"Please remove the following TimeSeries from the mapping file {humanize_collection(existing_result.nodes.as_ids())}"
                )
            total_validated += len(timeseries)
        print(
            f"Validated {total_validated:,} TimeSeries for migration. "
            f"{len(mappings):,} mappings provided in the mapping file."
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
                f" or delete some existing instances before proceeding with the migration of {len(mappings):,} timeseries."
            )
        total_instances = stats.instances.instances + len(mappings)
        self.console(
            f"Project has enough capacity for migration. Total instances after migration: {total_instances:,}."
        )

    @staticmethod
    def _confirm(mappings: MigrationMappingList) -> bool:
        print(
            Panel(
                f"[red]WARNING:[/red] This operation [bold]cannot be undone[/bold]! "
                f"{len(mappings):,} TimeSeries will linked to the new CogniteTimeSeries. "
                "This linking cannot be undone",
                style="bold",
                title="Migrate asset-centric TimeSeries to CogniteTimeSeries",
                title_align="left",
                border_style="red",
                expand=False,
            )
        )

        if not questionary.confirm("Are you really sure you want to continue?", default=False).ask():
            print("Migration cancelled by user.")
            return False
        return True

    def _migrate(self, client: ToolkitClient, mappings: MigrationMappingList, verbose: bool) -> None:
        print("Migrating TimeSeries to CogniteTimeSeries...")
        total_migrated = 0
        for chunk in track(
            chunker_sequence(mappings, size=self.chunk_size),
            description="Migrating TimeSeries to CogniteTimeSeries...",
            total=len(mappings) // self.chunk_size + 1,
        ):  # type: ignore[assignment]
            if verbose:
                print(f"Migrating {len(chunk):,} TimeSeries...")

            # Set pending IDs for the chunk of mappings
            try:
                pending_timeseries = client.time_series.set_pending_ids(chunk.as_pending_ids())
            except CogniteAPIError as e:
                raise ToolkitValueError(f"Failed to set pending IDs for TimeSeries: {e!s}") from e

            # The ExtendedTimeSeriesList is iterating ExtendedTimeSeries objects.
            converted_timeseries = [self.as_cognite_timeseries(ts) for ts in pending_timeseries]  # type: ignore[arg-type]
            try:
                created = client.data_modeling.instances.apply_fast(converted_timeseries)
            except CogniteAPIError as e:
                raise ToolkitValueError(f"Failed to apply TimeSeries: {e!s}") from e
            if verbose:
                print(f"Created {len(created):,} CogniteTimeSeries.")
            total_migrated += len(created)
        print(f"Successfully migrated {total_migrated:,} TimeSeries to CogniteTimeSeries.")

    @classmethod
    def read_mapping_file(cls, mapping_file: Path) -> MigrationMappingList:
        if not mapping_file.exists():
            raise ToolkitFileNotFoundError(f"Mapping file {mapping_file} does not exist.")
        if mapping_file.suffix != ".csv":
            raise ToolkitValueError(f"Mapping file {mapping_file} must be a CSV file.")
        with mapping_file.open(mode="r") as f:
            csv_file = csv.reader(f)
            header = next(csv_file, None)
            header = cls._validate_csv_header(header)
            if header[0] == "id":
                return cls._read_id_mapping(csv_file)
            else:  # header[0] == "externalId":
                return cls._read_external_id_mapping(csv_file)

    @classmethod
    def _validate_csv_header(cls, header: list[str] | None) -> list[str]:
        if header is None:
            raise ToolkitValueError("Mapping file is empty")
        errors: list[str] = []
        if len(header) < 3:
            errors.append("Mapping file must have at least 3 columns: id/externalId, space, externalId.")
        if len(header) >= 5:
            errors.append("Mapping file must have at most 4 columns: id/externalId, dataSetId, space, externalId.")
        if header[0] not in {"id", "externalId"}:
            errors.append("First column must be 'id' or 'externalId'.")
        if len(header) == 4 and header[1] != "dataSetId":
            errors.append("If there are 4 columns, the second column must be 'dataSetId'.")
        if header[-2:] != ["space", "externalId"]:
            errors.append("Last two columns must be 'space' and 'externalId'.")
        if errors:
            error_str = "\n - ".join(errors)
            raise ToolkitValueError(f"Invalid mapping file header:\n - {error_str}")
        return header

    @classmethod
    def _read_id_mapping(cls, csv_file: Iterator[list[str]]) -> MigrationMappingList:
        """Read a CSV file with ID mappings."""
        mappings = MigrationMappingList()
        for no, row in enumerate(csv_file, 1):
            try:
                id_ = int(row[0])
                data_set_id = int(row[1]) if len(row) == 4 and row[1] else None
            except ValueError as e:
                raise ToolkitValueError(
                    f"Invalid ID or dataSetId in row {no}: {row}. ID and dataSetId must be integers."
                ) from e
            instance_id = NodeId(*row[-2:])
            mappings.append(
                IdMigrationMapping(resource_type="timeseries", id=id_, instance_id=instance_id, data_set_id=data_set_id)
            )
        return mappings

    @classmethod
    def _read_external_id_mapping(cls, csv_file: Iterator[list[str]]) -> MigrationMappingList:
        """Read a CSV file with external ID mappings."""
        mappings = MigrationMappingList()
        for row in csv_file:
            external_id = row[0]
            data_set_id = int(row[1]) if len(row) == 4 and row[1] else None
            instance_id = NodeId(*row[-2:])
            mappings.append(
                ExternalIdMigrationMapping(
                    resource_type="timeseries",
                    external_id=external_id,
                    instance_id=instance_id,
                    data_set_id=data_set_id,
                )
            )
        return mappings

    @classmethod
    def as_cognite_timeseries(cls, ts: ExtendedTimeSeries) -> CogniteTimeSeriesApply:
        if ts.pending_instance_id is None:
            raise ToolkitValueError("ExtendedTimeSeries must have a pending_instance_id set before migration.")
        if ts.is_step is None:
            raise ToolkitValueError("ExtendedTimeSeries must have is_step set before migration.")

        return CogniteTimeSeriesApply(
            space=ts.pending_instance_id.space,
            external_id=ts.pending_instance_id.external_id,
            name=ts.name,
            description=ts.description,
            is_step=ts.is_step,
            time_series_type="string" if ts.is_string else "numeric",
            source_unit=ts.unit,
            unit=DirectRelationReference(cls.cdf_cdm_units, ts.unit_external_id) if ts.unit_external_id else None,
        )
