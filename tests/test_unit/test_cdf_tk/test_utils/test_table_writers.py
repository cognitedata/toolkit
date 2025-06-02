import csv
import re
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
    part_pattern = r"part-(?P<part>\d{4})"

    def get_part_number(self, file_path: Path) -> int:
        """Extract the part number from the file name."""
        # This is needed to sort files as Windows and Linux sorts numbers in file names differently.
        match = re.search(self.part_pattern, file_path.name)
        if match:
            return int(match.group("part"))
        return 0

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

    def test_write_csv_above_limit(self, example_schema: Schema, example_data: Rows, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        with CSVWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            for single_row in example_data:
                writer.write_rows([("group1", [single_row])])

        csv_files = sorted(output_dir.rglob("*.csv"), key=self.get_part_number)
        assert len(csv_files) == len(example_data)
        # Each row should be written to a separate file due to the size limit
        for csv_file, row in zip(csv_files, example_data):
            with csv_file.open("r", newline="") as f:
                reader = csv.reader(f)
                header = next(reader)
                actual_row = dict(zip(header, next(reader)))
                # csv writer stringifies all values, so we convert them back to their original types.
                expected_row = {key: str(value if value is not None else "") for key, value in row.items()}
                assert actual_row == expected_row

    def test_write_parquet_above_limit(self, example_schema: Schema, example_data: Rows, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        with ParquetWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            for single_row in example_data:
                writer.write_rows([("group1", [single_row])])

        parquet_files = sorted(output_dir.rglob("*.parquet"), key=self.get_part_number)
        assert len(parquet_files) == len(example_data)
        # Each row should be written to a separate file due to the size limit
        for parquet_file, row in zip(parquet_files, example_data):
            table = pq.read_table(parquet_file)
            assert table.num_rows == 1
            assert table.to_pylist() == [row]

    def test_write_yaml_above_limit(self, example_schema: Schema, example_data: Rows, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        with YAMLWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            for single_row in example_data:
                writer.write_rows([("group1", [single_row])])

        yaml_files = sorted(output_dir.rglob("*.yaml"), key=self.get_part_number)
        assert len(yaml_files) == len(example_data)
        for yaml_file, row in zip(yaml_files, example_data):
            with yaml_file.open("r", encoding="utf-8") as f:
                actual_data = yaml.safe_load(f)
                assert actual_data == [row]
