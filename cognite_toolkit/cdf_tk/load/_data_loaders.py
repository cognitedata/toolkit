from __future__ import annotations

import io
from pathlib import Path
from typing import final

import pandas as pd
from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability, FilesAcl, RawAcl, TimeSeriesAcl

from cognite_toolkit.cdf_tk.utils import CDFToolConfig

from ._base_loaders import DataLoader
from ._resource_loaders import FileMetadataLoader, RawDatabaseLoader, RawTableLoader, TimeSeriesLoader
from .data_classes import RawDatabaseTable


@final
class DatapointsLoader(DataLoader):
    folder_name = "timeseries_datapoints"
    filetypes = frozenset({"csv", "parquet"})
    dependencies = frozenset({TimeSeriesLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope: capabilities.AllScope | capabilities.DataSetScope = (
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All()
        )

        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            scope,
        )

    def upload(self, datafile: Path, dry_run: bool) -> str:
        if datafile.suffix == ".csv":
            # The replacement is used to ensure that we read exactly the same file on Windows and Linux
            file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
            data = pd.read_csv(io.StringIO(file_content), parse_dates=True, index_col=0)
            data.index = pd.DatetimeIndex(data.index)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(datafile, engine="pyarrow")
        else:
            raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")
        if dry_run:
            return f"Would insert {len(data)}x{len(data.columns)} datapoints from {datafile.name}"
        else:
            self.client.time_series.data.insert_dataframe(data)
            return f"Inserted {len(data)}x{len(data.columns)} datapoints from {datafile.name}"


@final
class FileLoader(DataLoader):
    folder_name = "files"
    filetypes = frozenset()
    filename_pattern = "^(yaml|yml)$"
    dependencies = frozenset({FileMetadataLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        scope: capabilities.AllScope | capabilities.DataSetScope
        if ToolGlobals.data_set_id is None:
            scope = FilesAcl.Scope.All()
        else:
            scope = FilesAcl.Scope.DataSet([ToolGlobals.data_set_id])

        return FilesAcl([FilesAcl.Action.Read, FilesAcl.Action.Write], scope)

    def upload(self, datafile: Path, dry_run: bool) -> str:
        if dry_run:
            return f"Would upload file {datafile.name}"
        else:
            self.client.files.upload(str(datafile), name=datafile.name, overwrite=True)
            return f"Uploaded file {datafile.name}"


@final
class RawFileLoader(DataLoader):
    folder_name = "raw"
    filename_pattern = "^(yaml|yml)$"
    filetypes = frozenset({"csv", "parquet"})
    dependencies = frozenset({RawDatabaseLoader, RawTableLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    def upload(self, datafile: Path, dry_run: bool) -> str:
        if any(
            (metadata_file := datafile.parent / f"{datafile.stem}{file_ending}").exists()
            for file_ending in [".yaml", ".yml"]
        ):
            metadata = RawDatabaseTable.load(metadata_file.read_text())
        else:
            raise ValueError(f"Missing metadata file for {datafile.name}. It should be named {datafile.stem}.yaml")

        if datafile.suffix == ".csv":
            # The replacement is used to ensure that we read exactly the same file on Windows and Linux
            file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
            data = pd.read_csv(io.StringIO(file_content), dtype=str)
            data.fillna("", inplace=True)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(datafile, engine="pyarrow")
        else:
            raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")

        if dry_run:
            return f"Would insert {len(data)}x{len(data.columns)} cells from {datafile.name}"

        if metadata.table_name is None:
            raise ValueError(f"Missing table name for {datafile.name}")
        self.client.raw.rows.insert_dataframe(metadata.db_name, metadata.table_name, data, ensure_parent=False)
        return f"Inserted {len(data)}x{len(data.columns)} cells from {datafile.name}"
