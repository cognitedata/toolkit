from collections.abc import Set
from typing import Any

import pytest
from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping, MigrationMappingList
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence, flatten_dict_json_path


class TestChunkSequence:
    def test_chunk_sequence(self):
        """Test that chunker_sequence correctly chunks a sequence."""
        sequence = list(range(10))
        chunk_size = 3
        expected_chunks = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [9],
        ]

        chunks = list(chunker_sequence(sequence, chunk_size))
        assert chunks == expected_chunks

    def test_chunk_sequence_empty(self):
        """Test that chunker_sequence returns an empty list for an empty sequence."""
        sequence = []
        chunk_size = 3

        chunks = list(chunker_sequence(sequence, chunk_size))
        assert chunks == []

    def test_chunk_sequence_maintain_source_type(self) -> None:
        """Test that chunker_sequence maintains the type of the source sequence."""
        sequence = MigrationMappingList(
            [
                MigrationMapping(
                    resourceType="timeseries", id=1, dataSetId=123, instanceId=NodeId("sp_full_ts", "full_ts_id")
                ),
                MigrationMapping(
                    resourceType="timeseries", id=2, dataSetId=None, instanceId=NodeId("sp_step_ts", "step_ts_id")
                ),
            ]
        )
        chunk_size = 1

        chunks = list(chunker_sequence(sequence, chunk_size))
        assert len(chunks) == 2
        assert len(chunks[0]) == 1
        assert len(chunks[1]) == 1
        assert isinstance(chunks[0], MigrationMappingList)


class TestFlattenDictJsonPath:
    @pytest.mark.parametrize(
        "dct,exclude_keys,expected",
        [
            pytest.param({"a": 1}, set(), {"a": 1}, id="flat dict"),
            pytest.param({"a": {"b": 2}}, set(), {"a.b": 2}, id="nested dict"),
            pytest.param(
                {"a": {"b": [{"c": {"d": 3}}, 1]}}, set(), {"a.b[0].c.d": 3, "a.b[1]": 1}, id="nested dict with list"
            ),
            pytest.param({}, set(), {}, id="empty dict"),
            pytest.param({"a": []}, set(), {}, id="empty list"),
            pytest.param({"a": {}}, set(), {}, id="nested empty dict"),
            pytest.param({"a": None}, set(), {"a": None}, id="None value"),
            pytest.param({"a": [1, 2, 3]}, set(), {"a[0]": 1, "a[1]": 2, "a[2]": 3}, id="simple list"),
            pytest.param(
                {"a": [1, [2, 3], 4]},
                set(),
                {"a[0]": 1, "a[1][0]": 2, "a[1][1]": 3, "a[2]": 4},
                id="nested list",
            ),
            pytest.param(
                {"a": [1, [2, 3], 4]},
                {"a"},
                {"a": [1, [2, 3], 4]},
                id="Exclude list key",
            ),
            pytest.param(
                {"a": {"b": {"c": {"d": {"e": 5}}}}},
                set(),
                {"a.b.c.d.e": 5},
                id="deep nesting",
            ),
            pytest.param(
                {"a": {"b": [{"c": [1, {"d": 2}]}, 3]}},
                set(),
                {"a.b[0].c[0]": 1, "a.b[0].c[1].d": 2, "a.b[1]": 3},
                id="complex nesting",
            ),
            pytest.param(
                {"a": {"b": True, "c": False}},
                set(),
                {"a.b": True, "a.c": False},
                id="boolean values",
            ),
            pytest.param(
                {
                    "name": "example",
                    "labels": [{"externalId": "equipment"}, {"externalId": "Fornebu"}],
                    "metadata": {"createdBy": "user"},
                },
                {"labels", "metadata"},
                {
                    "name": "example",
                    "labels": [{"externalId": "equipment"}, {"externalId": "Fornebu"}],
                    "metadata": {"createdBy": "user"},
                },
                id="Exclude specific keys",
            ),
        ],
    )
    def test_flatten_dict_json_path(self, dct: dict[str, Any], exclude_keys: Set[str], expected: dict[str, Any]):
        """Test that flatten_dict_json_path correctly flattens a nested dictionary."""
        flat_dict = flatten_dict_json_path(dct, keep_structured=exclude_keys)
        assert flat_dict == expected
