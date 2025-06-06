import csv
import re
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar

import pyarrow.parquet as pq
import pytest
import yaml
from cognite.client.data_classes.data_modeling import ContainerId, PropertyId, ViewId
from cognite.client.data_classes.data_modeling import data_types as dt
from cognite.client.data_classes.data_modeling.data_types import DirectRelationReference, EnumValue
from cognite.client.data_classes.data_modeling.views import (
    MappedProperty,
    MultiEdgeConnection,
    MultiReverseDirectRelation,
    SingleEdgeConnection,
    SingleReverseDirectRelation,
    ViewProperty,
)

from cognite_toolkit._cdf_tk.utils.table_writers import (
    CSVWriter,
    ParquetWriter,
    Rows,
    Schema,
    SchemaColumn,
    SchemaColumnList,
    YAMLWriter,
)


@pytest.fixture()
def example_schema() -> Schema:
    return Schema(
        display_name="Example",
        folder_name="example",
        kind="Example",
        format_="csv",
        columns=SchemaColumnList(
            [
                SchemaColumn(name="myString", type="string"),
                SchemaColumn(name="myInteger", type="integer"),
                SchemaColumn(name="myFloat", type="float"),
                SchemaColumn(name="myBoolean", type="boolean"),
                SchemaColumn(name="myJson", type="json"),
                SchemaColumn(name="myTimestamp", type="timestamp"),
                SchemaColumn(name="myDate", type="date"),
                SchemaColumn(name="myStringList", type="string", is_array=True),
                SchemaColumn(name="myIntegerList", type="integer", is_array=True),
                SchemaColumn(name="myFloatList", type="float", is_array=True),
                SchemaColumn(name="myBooleanList", type="boolean", is_array=True),
                SchemaColumn(name="myJsonList", type="json", is_array=False),
                SchemaColumn(name="myTimestampList", type="timestamp", is_array=True),
                SchemaColumn(name="myDateList", type="date", is_array=True),
            ]
        ),
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
            "myTimestamp": "2023-01-01T00:00:00Z",
            "myDate": "2023-01-01",
            "myStringList": ["on", "off", '"maybe"', "y", "n"],
            "myIntegerList": [1, 2],
            "myFloatList": [1.0, 2.0],
            "myBooleanList": [True, False],
            "myJsonList": [{"key1": "value1"}, {"key2": "value2"}],
            "myTimestampList": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
            "myDateList": ["2023-01-01", "2023-01-02"],
        },
        {
            "myString": "value2",
            "myInteger": None,
            "myFloat": 2.0,
            "myBoolean": False,
            "myJson": None,
            "myTimestamp": None,
            "myDate": None,
            "myStringList": None,
            "myIntegerList": None,
            "myFloatList": None,
            "myBooleanList": None,
            "myJsonList": None,
            "myTimestampList": None,
            "myDateList": None,
        },
        {
            "myString": None,
            "myInteger": 3,
            "myFloat": None,
            "myBoolean": True,
            "myJson": {"key": "value"},
            "myTimestamp": datetime(2023, 1, 3, 0, 0, 0),
            "myDate": date(2023, 1, 3),
            "myStringList": ["yes", "no"],
            "myIntegerList": [3, 4],
            "myFloatList": [3.0, 4.0],
            "myBooleanList": [True, False],
            "myJsonList": [{"key1": "value1"}, {"key2": "value2"}],
            "myTimestampList": [datetime(2023, 1, 3, 0, 0, 0), datetime(2023, 1, 4, 0, 0, 0)],
            "myDateList": [date(2023, 1, 3), date(2023, 1, 4)],
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
        # The parquet writer mutates the input data, so we need to deepcopy it.
        example_data_copy = deepcopy(example_data)

        with ParquetWriter(example_schema, output_dir) as writer:
            for single_row in example_data_copy:
                writer.write_rows([("group1", [single_row])])

        parquet_file = list(output_dir.rglob("*.parquet"))
        assert len(parquet_file) == 1
        table = pq.read_table(parquet_file[0])
        assert table.num_rows == len(example_data)
        actual_data = table.to_pylist()
        # We compare the mutated data, as the ParquetWriter standardizes the date, timestamp, and json formats.
        assert actual_data == example_data_copy

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
        # The parquet writer mutates the input data, so we need to deepcopy it.
        example_data_copy = deepcopy(example_data)
        with ParquetWriter(example_schema, output_dir, max_file_size_bytes=1) as writer:
            for single_row in example_data_copy:
                writer.write_rows([("group1", [single_row])])

        parquet_files = sorted(output_dir.rglob("*.parquet"), key=self.get_part_number)
        assert len(parquet_files) == len(example_data)
        # Each row should be written to a separate file due to the size limit
        # We compare the mutated data, as the ParquetWriter standardizes the date, timestamp, and json formats.
        for parquet_file, row in zip(parquet_files, example_data_copy):
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


class TestSchemaColumnList:
    container: ClassVar[ContainerId] = ContainerId("my_space", "my_container")
    view: ClassVar[ViewId] = ViewId("my_space", "my_view", "v1")
    default_values: ClassVar[Mapping[str, bool]] = dict(nullable=True, immutable=False, auto_increment=False)
    all_view_properties: Mapping[str, ViewProperty] = {
        "myText": MappedProperty(container, "myText", dt.Text(), **default_values),
        "myBoolean": MappedProperty(container, "myBoolean", dt.Boolean(), **default_values),
        "myTimestamp": MappedProperty(container, "myTimestamp", dt.Timestamp(), **default_values),
        "myDate": MappedProperty(container, "myDate", dt.Date(), **default_values),
        "myFloat32": MappedProperty(container, "myFloat32", dt.Float32(), **default_values),
        "myFloat64": MappedProperty(container, "myFloat64", dt.Float64(), **default_values),
        "myInt32": MappedProperty(container, "myInt32", dt.Int32(), **default_values),
        "myInt64": MappedProperty(container, "myInt64", dt.Int64(), **default_values),
        "myJson": MappedProperty(container, "myJson", dt.Json(), **default_values),
        "myStringList": MappedProperty(container, "myStringList", dt.Text(is_list=True), **default_values),
        "myIntegerList": MappedProperty(container, "myIntegerList", dt.Int64(is_list=True), **default_values),
        "myFloatList": MappedProperty(container, "myFloatList", dt.Float64(is_list=True), **default_values),
        "myBooleanList": MappedProperty(container, "myBooleanList", dt.Boolean(is_list=True), **default_values),
        "myJsonList": MappedProperty(container, "myJsonList", dt.Json(is_list=False), **default_values),
        "myDirectRelation": MappedProperty(container, "myDirectRelation", dt.DirectRelation(), **default_values),
        "myDirectRelationList": MappedProperty(
            container, "myDirectRelationList", dt.DirectRelation(is_list=True), **default_values
        ),
        "myTimeSeriesReference": MappedProperty(
            container, "myTimeSeriesReference", dt.TimeSeriesReference(), **default_values
        ),
        "myFileReference": MappedProperty(container, "myFileReference", dt.FileReference(), **default_values),
        "mySequenceReference": MappedProperty(
            container, "mySequenceReference", dt.SequenceReference(), **default_values
        ),
        "myTimeSeriesReferenceList": MappedProperty(
            container, "myTimeSeriesReferenceList", dt.TimeSeriesReference(is_list=True), **default_values
        ),
        "myFileReferenceList": MappedProperty(
            container, "myFileReferenceList", dt.FileReference(is_list=True), **default_values
        ),
        "mySequenceReferenceList": MappedProperty(
            container, "mySequenceReferenceList", dt.SequenceReference(is_list=True), **default_values
        ),
        "myEnum": MappedProperty(
            container, "myEnum", dt.Enum({"value1": EnumValue(), "value2": EnumValue()}), **default_values
        ),
        "myMultiEdge": MultiEdgeConnection(
            DirectRelationReference("my_space", "myMultiEdge"), view, None, None, None, "outwards"
        ),
        "mySingleEdge": SingleEdgeConnection(
            DirectRelationReference("my_space", "mySingleEdge"), view, None, None, None, "outwards"
        ),
        "myMultiReverseDirectRelation": MultiReverseDirectRelation(
            view, PropertyId(view, "myMultiReverseDirectRelation"), None, None
        ),
        "mySingleReverseDirectRelation": SingleReverseDirectRelation(
            view, PropertyId(view, "mySingleReverseDirectRelation"), None, None
        ),
    }

    def test_create_schema_from_view_properties(self) -> None:
        columns = SchemaColumnList.create_from_view_properties(self.all_view_properties)
        assert columns == SchemaColumnList(
            [
                SchemaColumn("space", "string"),
                SchemaColumn("externalId", "string"),
                SchemaColumn("instanceType", "string"),
                SchemaColumn("existingVersion", "integer"),
                SchemaColumn("type", "json"),
                SchemaColumn(name="properties.myText", type="string"),
                SchemaColumn(name="properties.myBoolean", type="boolean"),
                SchemaColumn(name="properties.myTimestamp", type="timestamp"),
                SchemaColumn(name="properties.myDate", type="date"),
                SchemaColumn(name="properties.myFloat32", type="float"),
                SchemaColumn(name="properties.myFloat64", type="float"),
                SchemaColumn(name="properties.myInt32", type="integer"),
                SchemaColumn(name="properties.myInt64", type="integer"),
                SchemaColumn(name="properties.myJson", type="json"),
                SchemaColumn(name="properties.myStringList", type="string", is_array=True),
                SchemaColumn(name="properties.myIntegerList", type="integer", is_array=True),
                SchemaColumn(name="properties.myFloatList", type="float", is_array=True),
                SchemaColumn(name="properties.myBooleanList", type="boolean", is_array=True),
                SchemaColumn(name="properties.myJsonList", type="json", is_array=False),
                SchemaColumn(name="properties.myDirectRelation", type="json"),
                SchemaColumn(name="properties.myDirectRelationList", type="json", is_array=False),
                SchemaColumn(name="properties.myTimeSeriesReference", type="string"),
                SchemaColumn(name="properties.myFileReference", type="string"),
                SchemaColumn(name="properties.mySequenceReference", type="string"),
                SchemaColumn(name="properties.myTimeSeriesReferenceList", type="string", is_array=True),
                SchemaColumn(name="properties.myFileReferenceList", type="string", is_array=True),
                SchemaColumn(name="properties.mySequenceReferenceList", type="string", is_array=True),
                SchemaColumn(name="properties.myEnum", type="string"),
            ]
        )

    def test_create_schema_from_view_properties_with_edge_support(self) -> None:
        columns = SchemaColumnList.create_from_view_properties(self.all_view_properties, support_edges=True)

        assert {SchemaColumn("startNode", "json"), SchemaColumn("endNode", "json")} <= set(columns)
