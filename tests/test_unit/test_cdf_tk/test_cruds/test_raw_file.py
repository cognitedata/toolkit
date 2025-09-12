from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.cruds import RawFileCRUD, RawTableCRUD
from cognite_toolkit._cdf_tk.data_classes import BuildEnvironment, BuiltResource, BuiltResourceList, SourceLocationEager


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
            loader = RawFileCRUD.create_loader(client)
        csv_file = MagicMock(spec=Path)
        csv_file.read_bytes.return_value = csv_content.encode("utf-8")
        csv_file.exists.return_value = True
        csv_file.suffix = ".csv"
        source_file = MagicMock(spec=Path)
        source_file.with_suffix.return_value = csv_file

        state = BuildEnvironment()
        state.built_resources[RawFileCRUD.folder_name] = BuiltResourceList(
            [
                BuiltResource(
                    RawTable("myDB", "myTable"),
                    SourceLocationEager(source_file, "1z234"),
                    RawTableCRUD.kind,
                    None,
                    None,
                )
            ]
        )

        list(loader.upload(state, dry_run=False))

        # Verify one upload call was made
        assert client.raw.rows.insert_dataframe.call_count == 1
        _, kwargs = client.raw.rows.insert_dataframe.call_args
        written_to_cdf = kwargs["dataframe"].to_dict(orient="index")
        # All values and types should match the expected ingestion payload
        assert written_to_cdf == expected_write

    def test_upload_preserves_numeric_types_and_sets_empty_strings_for_nulls(self) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = RawFileCRUD.create_loader(client)
        csv_file = MagicMock(spec=Path)
        csv_content = """myFloat,myInt,myString,myBool
,1,hello,True
0.2,,world,False
"""
        csv_file.read_bytes.return_value = csv_content.encode("utf-8")
        csv_file.exists.return_value = True
        csv_file.suffix = ".csv"
        source_file = MagicMock(spec=Path)
        source_file.with_suffix.return_value = csv_file

        state = BuildEnvironment()
        state.built_resources[RawFileCRUD.folder_name] = BuiltResourceList(
            [
                BuiltResource(
                    RawTable("myDB", "myTable"),
                    SourceLocationEager(source_file, "1z234"),
                    RawTableCRUD.kind,
                    None,
                    None,
                )
            ]
        )

        list(loader.upload(state, dry_run=False))

        # Capture the DataFrame uploaded to RAW

        # Verify one upload call was made
        assert client.raw.rows.insert_dataframe.call_count == 1
        _, kwargs = client.raw.rows.insert_dataframe.call_args
        df = kwargs["dataframe"]
        # Verify dtypes are object after astype(object).fillna("")
        assert str(df.dtypes["myFloat"]) == "object"
        assert str(df.dtypes["myInt"]) == "object"

        # Non-null float value remains numeric
        assert isinstance(df.iloc[1]["myFloat"], float)
        # CSV with nulls coerces an integer-like column to floats; value becomes 1.0
        assert isinstance(df.iloc[0]["myInt"], float)
        # Null in float column becomes empty string
        assert df.iloc[0]["myFloat"] == "" and isinstance(df.iloc[0]["myFloat"], str)
        # Null in int column becomes empty string
        assert df.iloc[1]["myInt"] == "" and isinstance(df.iloc[1]["myInt"], str)
