import csv
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from cognite_toolkit._cdf_tk.utils.table_writers import (
    CSVWriter,
    ParquetWriter,
    Rows,
    Schema,
    SchemaColumn,
    YAMLWriter,
)


@pytest.fixture()
def example_schema() -> Schema:
    return Schema(
        display_name="Example",
        folder_name="example",
        kind="Example",
        format_="csv",
        columns=[
            SchemaColumn(name="myString", type="string"),
            SchemaColumn(name="myInteger", type="integer"),
            SchemaColumn(name="myFloat", type="float"),
            SchemaColumn(name="myBoolean", type="boolean"),
            SchemaColumn(name="myJson", type="json"),
            SchemaColumn(name="myStringList", type="string", is_array=True),
            SchemaColumn(name="myIntegerList", type="integer", is_array=True),
            SchemaColumn(name="myFloatList", type="float", is_array=True),
            SchemaColumn(name="myBooleanList", type="boolean", is_array=True),
            SchemaColumn(name="myJsonList", type="json", is_array=False),
        ],
    )


@pytest.fixture()
def example_data() -> Rows:
    return [
        {
            "myString": "value1",
            "myInteger": 1,
            "myFloat": 1.0,
            "myBoolean": True,
            "myJson": {"key": "value"},
            "myStringList": ["on", "off", '"maybe"', "y", "n"],
            "myIntegerList": [1, 2],
            "myFloatList": [1.0, 2.0],
            "myBooleanList": [True, False],
            "myJsonList": [{"key1": "value1"}, {"key2": "value2"}],
        },
        {
            "myString": "value2",
            "myInteger": None,
            "myFloat": 2.0,
            "myBoolean": False,
            "myJson": None,
            "myStringList": None,
            "myIntegerList": None,
            "myFloatList": None,
            "myBooleanList": None,
            "myJsonList": None,
        },
        {
            "myString": None,
            "myInteger": 3,
            "myFloat": None,
            "myBoolean": True,
            "myJson": {"key": "value"},
            "myStringList": ["yes", "no"],
            "myIntegerList": [3, 4],
            "myFloatList": [3.0, 4.0],
            "myBooleanList": [True, False],
            "myJsonList": [{"key1": "value1"}, {"key2": "value2"}],
        },
    ]


class TestTableFileWriter:
    def test_write_csv(self, example_schema: Schema, example_data: Rows, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"

        with CSVWriter(example_schema, output_dir) as writer:
            for single_row in example_data:
                writer.write_rows([("group1", [single_row])])

        csv_file = list(output_dir.rglob("*.csv"))
        assert len(csv_file) == 1
        actual_data: Rows = []
        with csv_file[0].open("r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                actual_data.append(dict(zip(header, row)))

        # csv writer stringifies all values, so we convert them back to their original types.
        # Note at this stage (2. June 2025) Toolkit does not support a CSV reader.
        expected = [
            {key: str(value if value is not None else "") for key, value in row.items()} for row in example_data
        ]
        assert actual_data == expected

    def test_write_yaml(self, example_schema: Schema, example_data: Rows, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        example_schema.format_ = "yaml"

        with YAMLWriter(example_schema, output_dir) as writer:
            for single_row in example_data:
                writer.write_rows([("group1", [single_row])])

        yaml_file = list(output_dir.rglob("*.yaml"))
        assert len(yaml_file) == 1
        actual_data: Rows = yaml.safe_load(yaml_file[0].read_text(encoding="utf-8"))
        assert actual_data == example_data

    def test_write_parquet(self, example_schema: Schema, example_data: Rows, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        example_schema.format_ = "parquet"
        with ParquetWriter(example_schema, output_dir) as writer:
            for single_row in example_data:
                writer.write_rows([("group1", [single_row])])

        parquet_file = list(output_dir.rglob("*.parquet"))
        assert len(parquet_file) == 1
        table = pq.read_table(parquet_file[0])
        assert table.num_rows == len(example_data)
        actual_data = table.to_pylist()
        assert actual_data == example_data

    def test_write_csv_above_limit(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        with CSVWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
            writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        csv_files = list(output_dir.rglob("*.csv"))
        assert len(csv_files) == 2

    def test_write_parquet_above_limit(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        with ParquetWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
            writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        parquet_files = list(output_dir.rglob("*.parquet"))
        assert len(parquet_files) == 2

    def test_write_yaml_above_limit(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        with YAMLWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
            writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        yaml_files = list(output_dir.rglob("*.yaml"))
        assert len(yaml_files) == 2
