from __future__ import annotations

import io
from pathlib import Path
from typing import final

import pandas as pd
from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability, TimeSeriesAcl

from cognite_toolkit.cdf_tk.utils import CDFToolConfig

from ._base_loaders import DataLoader
from ._resource_loaders import TimeSeriesLoader


@final
class DatapointsLoader(DataLoader):
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

    def upload(self, datafile: Path) -> bool:
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
        return True
