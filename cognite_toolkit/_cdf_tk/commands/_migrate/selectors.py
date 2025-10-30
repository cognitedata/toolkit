from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Literal

from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMappingList
from cognite_toolkit._cdf_tk.storageio import DataSelector
from cognite_toolkit._cdf_tk.storageio.selectors import DataSetSelector
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricKind


class AssetCentricMigrationSelector(DataSelector, ABC):
    kind: AssetCentricKind

    @abstractmethod
    def get_ingestion_mappings(self) -> list[str]:
        raise NotImplementedError()


class MigrationCSVFileSelector(AssetCentricMigrationSelector):
    type: Literal["migrationCSVFile"] = "migrationCSVFile"
    datafile: Path

    @property
    def group(self) -> str:
        return f"Migration_{self.kind}"

    def __str__(self) -> str:
        return f"file_{self.datafile.name}"

    def get_ingestion_mappings(self) -> list[str]:
        views = {item.get_ingestion_view() for item in self.items}
        return sorted(views)

    @cached_property
    def items(self) -> MigrationMappingList:
        return MigrationMappingList.read_csv_file(self.datafile, resource_type=self.kind)


class MigrateDataSetSelector(AssetCentricMigrationSelector):
    type: Literal["migrateDataSet"] = "migrateDataSet"
    kind: AssetCentricKind
    data_set_external_id: str
    ingestion_mapping: str | None = None
    preferred_consumer_view: ViewId | None = None

    @property
    def group(self) -> str:
        return f"DataSet_{self.data_set_external_id}"

    def __str__(self) -> str:
        return self.kind

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return None

    def get_ingestion_mappings(self) -> list[str]:
        return [self.ingestion_mapping] if self.ingestion_mapping else []

    def as_asset_centric_selector(self) -> DataSetSelector:
        return DataSetSelector(data_set_external_id=self.data_set_external_id, kind=self.kind)
