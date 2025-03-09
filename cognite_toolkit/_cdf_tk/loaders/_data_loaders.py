from __future__ import annotations

import io
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, cast, final

import pandas as pd
from cognite.client.data_classes import FileMetadataWrite
from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass

from cognite_toolkit._cdf_tk.client.data_classes.extendable_cognite_file import ExtendableCogniteFileApply
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.utils import read_yaml_content, safe_read
from cognite_toolkit._cdf_tk.utils.file import read_csv

from ._base_loaders import T_ID, DataLoader, ResourceLoader, T_WritableCogniteResourceList
from ._resource_loaders import CogniteFileLoader, FileMetadataLoader, RawTableLoader, TimeSeriesLoader

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.data_classes import BuildEnvironment


@final
class DatapointsLoader(DataLoader):
    item_name = "datapoints"
    folder_name = "timeseries"
    kind = "Datapoints"
    filetypes = frozenset({"csv", "parquet"})
    dependencies = frozenset({TimeSeriesLoader})
    _doc_url = "Time-series/operation/postMultiTimeSeriesDatapoints"

    @property
    def display_name(self) -> str:
        return "timeseries datapoints"

    def upload(self, state: BuildEnvironment, dry_run: bool) -> Iterable[tuple[str, int]]:
        if self.folder_name not in state.built_resources:
            return

        resource_directories = state.built_resources[self.folder_name].get_resource_directories(self.folder_name)

        for resource_dir in resource_directories:
            for datafile in self._find_data_files(resource_dir):
                if datafile.suffix == ".csv":
                    # The replacement is used to ensure that we read exactly the same file on Windows and Linux
                    file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
                    data = read_csv(io.StringIO(file_content), parse_dates=True, index_col=0)
                    data.index = pd.DatetimeIndex(data.index)
                elif datafile.suffix == ".parquet":
                    data = pd.read_parquet(datafile, engine="pyarrow")
                else:
                    raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")
                timeseries_ids = list(data.columns)
                if len(timeseries_ids) == 1:
                    ts_str = timeseries_ids[0]
                elif len(timeseries_ids) <= 10:
                    ts_str = str(timeseries_ids)
                else:
                    ts_str = f"{len(timeseries_ids):,} timeseries"

                if data.empty:
                    yield (
                        f"Empty file {datafile.as_posix()!r}. No datapoints to inserted.",
                        0,
                    )
                    continue

                if dry_run:
                    yield (
                        f" Would insert '{len(data):,}x{len(data.columns):,}' datapoints from '{datafile!s}' into {ts_str}",
                        len(data) * len(data.columns),
                    )
                else:
                    self.client.time_series.data.insert_dataframe(data)

                    yield (
                        f" Inserted '{len(data):,}x{len(data.columns):,}' datapoints from '{datafile!s}' into {ts_str}",
                        len(data) * len(data.columns),
                    )


@final
class FileLoader(DataLoader):
    item_name = "file contents"
    folder_name = "files"
    kind = "File"
    filetypes = frozenset()
    exclude_filetype: frozenset[str] = frozenset({})
    filename_pattern = (
        # Exclude FileMetadata and CogniteFile
        r"(?i)^(?!.*(?:FileMetadata|CogniteFile)$).*$"
    )
    dependencies = frozenset({FileMetadataLoader, CogniteFileLoader})
    _doc_url = "Files/operation/initFileUpload"

    @property
    def display_name(self) -> str:
        return "file content"

    def upload(self, state: BuildEnvironment, dry_run: bool) -> Iterable[tuple[str, int]]:
        if self.folder_name not in state.built_resources:
            return

        for resource in state.built_resources[self.folder_name]:
            if resource.destination is None:
                continue

            if result := {
                FileMetadataLoader.kind: (FileMetadataLoader, "external_id"),
                CogniteFileLoader.kind: (CogniteFileLoader, "instance_id"),
            }.get(resource.kind):
                loader_cls, id_name = result
                meta: FileMetadataWrite | ExtendableCogniteFileApply = self._read_metadata(
                    resource.destination,
                    loader_cls,  # type: ignore[arg-type]
                    resource.identifier,
                )
                if meta.name is None:
                    continue
                datafile = resource.source.path.parent / meta.name
                if not datafile.exists():
                    continue

                if dry_run:
                    yield f" Would upload file '{datafile!s}' to file with {id_name}={resource.identifier!r}", 1
                else:
                    self.client.files.upload_content(path=str(datafile), **{id_name: resource.identifier})
                    yield f" Uploaded file '{datafile!s}' to file with {id_name}={resource.identifier!r}", 1

    @staticmethod
    def _read_metadata(
        destination: Path,
        loader: type[
            ResourceLoader[
                T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
            ]
        ],
        identifier: T_ID,
    ) -> T_WriteClass:
        built_content = read_yaml_content(safe_read(destination))
        if isinstance(built_content, dict):
            return loader.resource_write_cls.load(built_content)
        elif isinstance(built_content, list):
            try:
                return next(m for m in loader.list_write_cls.load(built_content) if loader.get_id(m) == identifier)
            except StopIteration:
                raise RuntimeError(f"Missing metadata for {destination.as_posix()}")

        raise RuntimeError(f"Unexpected content type {type(built_content)} in {destination.as_posix()}")


@final
class RawFileLoader(DataLoader):
    item_name = "rows"
    folder_name = "raw"
    filetypes = frozenset({"csv", "parquet"})
    kind = "Raw"
    dependencies = frozenset({RawTableLoader})
    _doc_url = "Raw/operation/postRows"

    @property
    def display_name(self) -> str:
        return "raw rows"

    def upload(self, state: BuildEnvironment, dry_run: bool) -> Iterable[tuple[str, int]]:
        if self.folder_name not in state.built_resources:
            return

        for resource in state.built_resources[self.folder_name]:
            if resource.kind != RawTableLoader.kind:
                continue
            table = cast(RawTable, resource.identifier)
            datafile = next(
                (
                    resource.source.path.with_suffix(f".{file_type}")
                    for file_type in self.filetypes
                    if (resource.source.path.with_suffix(f".{file_type}").exists())
                ),
                None,
            )
            if datafile is None:
                # No adjacent data file found
                continue

            if datafile.suffix == ".csv":
                # The replacement is used to ensure that we read exactly the same file on Windows and Linux
                file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
                data = read_csv(io.StringIO(file_content))
                data.fillna("", inplace=True)
                if not data.columns.empty and data.columns[0] == "key":
                    print(f"Setting index to 'key' for {datafile.name}")
                    data.set_index("key", inplace=True)
            elif datafile.suffix == ".parquet":
                data = pd.read_parquet(datafile, engine="pyarrow")
            else:
                raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")

            if data.empty:
                yield (
                    f"Empty file {datafile.as_posix()!r}. No rows to insert into {table!r}.",
                    0,
                )
                continue

            if dry_run:
                yield (
                    (
                        f" Would insert {len(data):,} rows of {len(data.columns):,} columns from '{datafile!s}' "
                        f"into {table!r}."
                    ),
                    len(data),
                )
                continue

            self.client.raw.rows.insert_dataframe(
                db_name=table.db_name, table_name=table.table_name, dataframe=data, ensure_parent=False
            )
            yield (
                (f" Inserted {len(data):,} rows of {len(data.columns):,} columns from '{datafile!s}' into {table!r}."),
                len(data),
            )
