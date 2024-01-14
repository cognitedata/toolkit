from __future__ import annotations

import io
from collections.abc import Sequence
from pathlib import Path
from typing import final

import pandas as pd
from cognite.client.data_classes import TimeSeries, TimeSeriesList, TimeSeriesWriteList, capabilities
from cognite.client.data_classes.capabilities import Capability, TimeSeriesAcl
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit.cdf_tk.load import Loader, TimeSeriesLoader
from cognite_toolkit.cdf_tk.utils import CDFToolConfig


@final
class DatapointsLoader(Loader[list[str], Path, Path, TimeSeriesWriteList, TimeSeriesList]):  # type: ignore[type-var]
    support_drop = False
    filetypes = frozenset({"csv", "parquet"})
    api_name = "time_series.data"
    folder_name = "timeseries_datapoints"
    resource_cls = pd.DataFrame
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

    @classmethod
    def get_id(cls, item: Path) -> list[str]:
        raise NotImplementedError

    def load_resource(self, filepath: Path, skip_validation: bool) -> Path:
        return filepath

    def create(self, items: Sequence[Path], drop: bool, filepath: Path) -> TimeSeriesList:
        if len(items) != 1:
            raise ValueError("Datapoints must be loaded one at a time.")
        datafile = items[0]
        if datafile.suffix == ".csv":
            # The replacement is used to ensure that we read exactly the same file on Windows and Linux
            file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
            data = pd.read_csv(io.StringIO(file_content), parse_dates=True, index_col=0)
            data.index = pd.DatetimeIndex(data.index)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(datafile, engine="pyarrow")
        else:
            raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")
        self.client.time_series.data.insert_dataframe(data)
        external_ids = [col for col in data.columns if not pd.api.types.is_datetime64_any_dtype(data[col])]
        return TimeSeriesList([TimeSeries(external_id=external_id) for external_id in external_ids])

    def update(self, items: Sequence[Path], filepath: Path) -> TimeSeriesList:
        raise NotImplementedError("Datapoints do not support update.")

    def delete(self, ids: SequenceNotStr[list[str]], drop_data: bool) -> int:
        # Drop all datapoints?
        raise NotImplementedError()
