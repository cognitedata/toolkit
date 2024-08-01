from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
import questionary
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    DataSetWrite,
    DataSetWriteList,
    TimeSeriesFilter,
    TimeSeriesList,
)
from cognite.client.exceptions import CogniteAPIError
from rich.progress import Progress, TaskID

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitMissingResourceError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import DataSetsLoader
from cognite_toolkit._cdf_tk.prototypes.resource_loaders import TimeSeriesLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig

#
# TODO: what is the naming convention for TimeSeries, timeseries, time_series, singular or plural?
#


class DumpTimeSeriesCommand(ToolkitCommand):
    # 128 MB
    buffer_size = 128 * 1024 * 1024
    # Note the size in memory is not the same as the size on disk,
    # so the resulting file size will vary.
    encoding = "utf-8"
    newline = "\n"

    def __init__(self, print_warning: bool = True, skip_tracking: bool = False):
        super().__init__(print_warning, skip_tracking)
        self.time_series_external_id_by_id: dict[int, str] = {}
        self.data_set_by_id: dict[int, DataSetWrite] = {}

        # TODO: not storing the whole asset, only external_id to not blow mem
        # - but maybe all assets should be dumped too, like assets>datasets?
        self.asset_by_id: dict[int, str] = {}
        self._used_assets: set[int] = set()

        self._used_data_sets: set[int] = set()
        self._available_data_sets: set[str] | None = None

    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        data_set: list[str] | None,
        interactive: bool,
        output_dir: Path,
        clean: bool,
        limit: int | None = None,
        format_: Literal["yaml", "csv", "parquet"] = "csv",
        verbose: bool = False,
    ) -> None:
        if format_ not in {"yaml", "csv", "parquet"}:
            raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are yaml, csv, parquet.")
        if output_dir.exists() and clean:
            shutil.rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")
        elif output_dir.suffix:
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")

        data_sets = self._select_data_set(ToolGlobals.client, data_set, interactive)
        if not data_sets:
            raise ToolkitValueError("No data set provided")

        if missing := set(data_sets) - {item.external_id for item in self.data_set_by_id.values() if item.external_id}:
            try:
                retrieved = ToolGlobals.client.data_sets.retrieve_multiple(external_ids=list(missing))
            except CogniteAPIError as e:
                raise ToolkitMissingResourceError(f"Failed to retrieve data sets {data_sets}: {e}")

            self.data_set_by_id.update({item.id: item.as_write() for item in retrieved if item.id})

        (output_dir / TimeSeriesLoader.folder_name).mkdir(parents=True, exist_ok=True)

        total_time_series = ToolGlobals.client.time_series.aggregate_count(
            filter=TimeSeriesFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
            )
        )
        if limit:
            total_time_series = min(total_time_series, limit)

        with Progress() as progress:
            retrieved_time_series = progress.add_task("Retrieving time_series", total=total_time_series)
            write_to_file = progress.add_task("Writing time_series to file(s)", total=total_time_series)

            time_series_iterator = ToolGlobals.client.time_series(
                chunk_size=1000,
                data_set_external_ids=data_sets or None,
                limit=limit,
            )
            time_series_iterator = self._log_retrieved(time_series_iterator, progress, retrieved_time_series)
            writeable = self._to_write(time_series_iterator, ToolGlobals.client, expand_metadata=True)

            count = 0
            if format_ == "yaml":
                for time_series in writeable:
                    file_path = output_dir / TimeSeriesLoader.folder_name / f"TimeSeries.{format_}"
                    if file_path.exists():
                        with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                            f.write("\n")
                            f.write(yaml.safe_dump(time_series, sort_keys=False))
                    else:
                        with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                            f.write(yaml.safe_dump(time_series, sort_keys=False))
                    count += len(time_series)
                    progress.advance(write_to_file, advance=len(time_series))
            elif format_ in {"csv", "parquet"}:
                file_count: int = 0  # Counter()
                for df in self._buffer(writeable):
                    folder_path = output_dir / TimeSeriesLoader.folder_name
                    folder_path.mkdir(parents=True, exist_ok=True)
                    file_path = folder_path / f"part-{file_count:04}.TimeSeries.{format_}"
                    if format_ == "csv":
                        df.to_csv(
                            file_path,
                            index=False,
                            encoding=self.encoding,
                            lineterminator=self.newline,
                        )
                    elif format_ == "parquet":
                        df.to_parquet(file_path, index=False)
                    file_count += 1
                    if verbose:
                        print(f"Dumped {len(df):,} time_series to {file_path}")
                    count += len(df)
                    progress.advance(write_to_file, advance=len(df))
            else:
                raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are yaml, csv, parquet. ")

        print(f"Dumped {count:,} time_series to {output_dir}")

        if self._used_data_sets:
            to_dump = DataSetWriteList(
                [self.data_set_by_id[used_dataset] for used_dataset in self._used_data_sets]
            ).dump_yaml()
            file_path = output_dir / DataSetsLoader.folder_name / "time_series.DataSet.yaml"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if file_path.exists():
                with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                    f.write("\n")
                    f.write(to_dump)
            else:
                with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                    f.write(to_dump)

            print(f"Dumped {len(self.data_set_by_id):,} data sets to {file_path}")

    def _buffer(self, time_series_iterator: Iterator[list[dict[str, Any]]]) -> Iterator[pd.DataFrame]:
        """Iterates over time_series until the buffer reaches the filesize."""
        stored_time_series: pd.DataFrame = pd.DataFrame()
        for time_series in time_series_iterator:
            stored_time_series = pd.concat(
                [stored_time_series, pd.DataFrame(time_series)],
                ignore_index=True,
            )
            if stored_time_series.memory_usage().sum() > self.buffer_size:
                yield stored_time_series
                # reset of the buffer required
                stored_time_series = pd.DataFrame()
        if not stored_time_series.empty:
            yield stored_time_series

    def _select_data_set(
        self,
        client: CogniteClient,
        data_set: list[str] | None,
        interactive: bool,
    ) -> list[str]:
        if not interactive:
            return data_set or []

        data_sets: set[str] = set()
        while True:
            selected = []
            if data_sets:
                selected.append(f"Selected data sets: {sorted(data_sets)}")
            else:
                selected.append("No data set selected.")
            selected_str = "\n".join(selected)
            what = questionary.select(
                f"\n{selected_str}\nSelect a data set to dump",
                choices=[
                    "Data Set",
                    "Done",
                ],
            ).ask()

            if what == "Done":
                break
            elif what == "Data Set":
                _available_data_sets = self._get_available_data_sets(client)
                selected_data_set = questionary.checkbox(
                    "Select a data set",
                    choices=sorted(item for item in _available_data_sets if item not in data_sets),
                ).ask()
                if selected_data_set:
                    data_sets.update(selected_data_set)
                else:
                    print("No data set selected.")
        return list(data_sets)

    def _get_available_data_sets(self, client: CogniteClient) -> set[str]:
        if self._available_data_sets is None:
            self.data_set_by_id.update({item.id: item.as_write() for item in client.data_sets})
            self._available_data_sets = {item.external_id for item in self.data_set_by_id.values() if item.external_id}
        return self._available_data_sets

    def _to_write(
        self,
        time_series: Iterator[TimeSeriesList],
        client: CogniteClient,
        expand_metadata: bool,
    ) -> Iterator[list[Any]]:
        for time_series_list in time_series:
            write_time_series: list[dict[str, Any]] = []
            # TODO: how to separate `times_series` as list from `time_series` as one item?
            for one_time_series in time_series_list:
                write = one_time_series.as_write().dump(camel_case=True)
                write.pop("parentId", None)
                if "dataSetId" in write:
                    data_set_id = write.pop("dataSetId")
                    self._used_data_sets.add(data_set_id)
                    write["dataSetExternalId"] = self._get_data_set_external_id(client, data_set_id)
                if "assetId" in write:
                    asset_id = write.pop("assetId")
                    self._used_assets.add(asset_id)
                    write["assetExternalId"] = self._get_asset_external_id(client, asset_id)
                if expand_metadata and "metadata" in write:
                    metadata = write.pop("metadata")
                    for key, value in metadata.items():
                        write[f"metadata.{key}"] = value
                if "rootId" in write:
                    root_id = write.pop("rootId")
                    write["rootExternalId"] = self._get_time_series_external_id(client, root_id)
                write_time_series.append(write)
            yield write_time_series

    def _get_time_series_external_id(self, client: CogniteClient, root_id: int) -> str:
        if root_id in self.time_series_external_id_by_id:
            return self.time_series_external_id_by_id[root_id]
        try:
            time_series = client.time_series.retrieve(id=root_id)
        except CogniteAPIError as e:
            raise ToolkitMissingResourceError(f"Failed to retrieve time_series {root_id}: {e}")
        if time_series is None:
            raise ToolkitMissingResourceError(f"TimeSeries {root_id} does not exist")
        if not time_series.external_id:
            raise ToolkitValueError(f"TimeSeries {root_id} does not have an external id")
        self.time_series_external_id_by_id[root_id] = time_series.external_id
        return time_series.external_id

    def _get_data_set_external_id(self, client: CogniteClient, data_set_id: int) -> str:
        if data_set_id in self.data_set_by_id:
            return cast(str, self.data_set_by_id[data_set_id].external_id)
        try:
            data_set = client.data_sets.retrieve(id=data_set_id)
        except CogniteAPIError as e:
            raise ToolkitMissingResourceError(f"Failed to retrieve data set {data_set_id}: {e}")
        if data_set is None:
            raise ToolkitMissingResourceError(f"Data set {data_set_id} does not exist")
        if not data_set.external_id:
            raise ToolkitValueError(f"Data set {data_set_id} does not have an external id")
        self.data_set_by_id[data_set_id] = data_set.as_write()
        return data_set.external_id

    def _get_asset_external_id(self, client: CogniteClient, asset_id: int) -> str:
        if asset_id in self.asset_by_id:
            return cast(str, self.asset_by_id[asset_id])
        try:
            asset = client.assets.retrieve(id=asset_id)
        except CogniteAPIError as e:
            raise ToolkitMissingResourceError(f"Failed to retrieve data set {asset_id}: {e}")
        if asset is None:
            raise ToolkitMissingResourceError(f"Data set {asset_id} does not exist")
        if not asset.external_id:
            raise ToolkitValueError(f"Data set {asset_id} does not have an external id")
        # TODO: not like data_set storing the whole `asset.as_write()` to not blow the memory
        # self.asset_by_id[asset_id] = asset.as_write()
        self.asset_by_id[asset_id] = asset.external_id
        return asset.external_id

    @staticmethod
    def _log_retrieved(
        time_series_iterator: Iterator[TimeSeriesList], progress: Progress, task: TaskID
    ) -> Iterator[TimeSeriesList]:
        for time_series_list in time_series_iterator:
            progress.advance(task, advance=len(time_series_list))
            yield time_series_list
