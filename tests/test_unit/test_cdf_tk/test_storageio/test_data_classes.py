from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import EdgeId, NodeId

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceIdList


class TestInstanceIdList:
    def test_read_csv_file(self, tmp_path: Path) -> None:
        csv_content = (
            "space,externalId,instanceType\nmy_space,instance1,node\nmy_space,instance2,node\nmy_space,instance3,edge\n"
        )
        csv_file = tmp_path / "instances.csv"
        csv_file.write_text(csv_content)

        instance_list = InstanceIdList.read_csv_file(csv_file)
        assert len(instance_list.invalid_rows) == 0
        assert len(instance_list) == 3
        assert instance_list == InstanceIdList(
            [NodeId(space="my_space", external_id=f"instance{i}") for i in range(1, 3)]
            + [EdgeId(space="my_space", external_id="instance3")]
        )
        sublist = instance_list[:2]
        assert isinstance(sublist, InstanceIdList)
        assert sublist == InstanceIdList([NodeId(space="my_space", external_id=f"instance{i}") for i in range(1, 3)])
        assert instance_list[0] == NodeId(space="my_space", external_id="instance1")

    def test_read_csv_file_with_errors(self, tmp_path: Path) -> None:
        csv_content = "space,externalId,instanceType\nmy_space,instance1,node\n,instance2,node\nmy_space,,edge\nmy_space,instance4,invalid_type\n,,"
        csv_file = tmp_path / "instances.csv"
        csv_file.write_text(csv_content)

        instance_list = InstanceIdList.read_csv_file(csv_file)
        assert len(instance_list) == 1
        assert instance_list == InstanceIdList([NodeId(space="my_space", external_id="instance1")])
        assert instance_list.invalid_rows == {
            2: ["Space is empty."],
            3: ["External ID is empty."],
            4: ["Unknown instance type 'invalid_type', expected 'node' or 'edge'."],
            5: ["Space is empty.", "External ID is empty.", "Unknown instance type '', expected 'node' or 'edge'."],
        }

    @pytest.mark.parametrize(
        "csv_content,expected_error",
        [
            pytest.param(
                "space,externalId\nmy_space,instance1\n", "Missing required columns: instanceType", id="Missing column"
            ),
            pytest.param("space,externalId,InstanceType\n", "No data found in the file: '{filepath}'", id="Empty file"),
        ],
    )
    def test_read_csv_file_missing_column(self, csv_content: str, expected_error: str, tmp_path: Path) -> None:
        csv_file = tmp_path / "instances.csv"
        csv_file.write_text(csv_content)
        if "{filepath}" in expected_error:
            expected_error = expected_error.format(filepath=csv_file.as_posix())

        with pytest.raises(ToolkitValueError) as e:
            InstanceIdList.read_csv_file(csv_file)

        assert str(e.value) == expected_error
