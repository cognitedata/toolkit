from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.data_classes import BuildEnvironment, BuiltResource, BuiltResourceList, SourceLocationEager
from cognite_toolkit._cdf_tk.loaders import RawFileLoader, RawTableLoader


class TestRawFileLoader:
    @pytest.mark.parametrize(
        "csv_content, expected_write",
        [
            (
                """myFloat,myInt,myString,myBool
0.1,1,hello,True
0.2,2,world,False
""",
                {
                    0: {"myFloat": 0.1, "myInt": 1, "myString": "hello", "myBool": True},
                    1: {"myFloat": 0.2, "myInt": 2, "myString": "world", "myBool": False},
                },
            )
        ],
    )
    def test_upload_dtypes(self, csv_content: str, expected_write: dict[int, Any]) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = RawFileLoader.create_loader(client)
        csv_file = MagicMock(spec=Path)
        csv_file.read_bytes.return_value = csv_content.encode("utf-8")
        csv_file.exists.return_value = True
        csv_file.suffix = ".csv"
        source_file = MagicMock(spec=Path)
        source_file.with_suffix.return_value = csv_file

        state = BuildEnvironment()
        state.built_resources[RawFileLoader.folder_name] = BuiltResourceList(
            [
                BuiltResource(
                    RawTable("myDB", "myTable"),
                    SourceLocationEager(source_file, "1z234"),
                    RawTableLoader.kind,
                    None,
                    None,
                )
            ]
        )

        list(loader.upload(state, dry_run=False))

        assert client.raw.rows.insert_dataframe.call_count == 1
        _, kwargs = client.raw.rows.insert_dataframe.call_args
        written_to_cdf = kwargs["dataframe"].to_dict(orient="index")
        assert written_to_cdf == expected_write
