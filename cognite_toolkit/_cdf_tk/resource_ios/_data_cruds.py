import io
from collections.abc import Iterable
from typing import TYPE_CHECKING, cast, final

import pandas as pd

from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWTableResponse
from cognite_toolkit._cdf_tk.utils.file import read_csv

from ._base_ios import DataCRUD
from ._resource_ios import RawTableCRUD

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.data_classes import BuildEnvironment


@final
class RawFileCRUD(DataCRUD):
    item_name = "rows"
    folder_name = "raw"
    kind = "Raw"
    dependencies = frozenset({RawTableCRUD})
    _doc_url = "Raw/operation/postRows"

    @property
    def display_name(self) -> str:
        return "raw rows"

    def upload(self, state: "BuildEnvironment", dry_run: bool) -> Iterable[tuple[str, int]]:
        if self.folder_name not in state.built_resources:
            return

        for resource in state.built_resources[self.folder_name]:
            if resource.kind != RawTableCRUD.kind:
                continue
            table = cast(RAWTableResponse, resource.identifier)
            datafile = next(
                (
                    resource.source.path.with_suffix(f".{file_type}")
                    for file_type in ["csv", "parquet"]
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
                data = read_csv(io.StringIO(file_content)).astype(object).fillna("")
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
                db_name=table.db_name, table_name=table.name, dataframe=data, ensure_parent=False
            )
            yield (
                (f" Inserted {len(data):,} rows of {len(data.columns):,} columns from '{datafile!s}' into {table!r}."),
                len(data),
            )
