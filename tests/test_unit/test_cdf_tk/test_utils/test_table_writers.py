from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cognite_toolkit._cdf_tk.utils.table_writers import CSVWriter, ParquetWriter, Schema, SchemaColumn, TableFileWriter


@pytest.fixture()
def example_schema() -> Schema:
    return Schema(
        display_name="Example",
        folder_name="example",
        kind="Example",
        format_="csv",
        columns=[
            SchemaColumn(name="column1", type="string"),
            SchemaColumn(name="column2", type="integer"),
            SchemaColumn(name="column3", type="float"),
        ],
    )


class TestTableFileWriter:
    def test_write_csv(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        writer = TableFileWriter.load(example_schema, output_dir)

        writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
        writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        csv_file = list(output_dir.rglob("*.csv"))
        assert len(csv_file) == 1
        actual_file = csv_file[0].read_text()
        assert actual_file == ("column1,column2,column3\nvalue1,1,1.0\nvalue2,,2.0\n")

    def test_write_yaml(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        example_schema.format_ = "yaml"
        writer = TableFileWriter.load(example_schema, output_dir)

        writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
        writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        yaml_file = list(output_dir.rglob("*.yaml"))
        assert len(yaml_file) == 1
        actual_file = yaml_file[0].read_text()
        assert actual_file == ("- column1: value1\n  column2: 1\n  column3: 1.0\n- column1: value2\n  column3: 2.0\n")

    def test_write_parquet(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        example_schema.format_ = "parquet"
        with ParquetWriter(example_schema, output_dir) as writer:
            writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
            writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        parquet_file = list(output_dir.rglob("*.parquet"))
        assert len(parquet_file) == 1
        table = pq.read_table(parquet_file[0])
        assert table.num_rows == 2
        assert table.column("column1").to_pylist() == ["value1", "value2"]
        assert table.column("column2").to_pylist() == [1, None]
        assert table.column("column3").to_pylist() == [1.0, 2.0]

    def test_write_csv_above_limit(self, example_schema: Schema, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        writer = CSVWriter(example_schema, output_dir, max_file_size_bytes=1)

        writer.write_rows([("group1", [{"column1": "value1", "column2": 1, "column3": 1.0}])])
        writer.write_rows([("group1", [{"column1": "value2", "column3": 2.0}])])

        csv_files = list(output_dir.rglob("*.csv"))
        assert len(csv_files) == 2
