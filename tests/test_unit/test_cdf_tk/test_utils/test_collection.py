from collections.abc import Set
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping, MigrationMappingList
from cognite_toolkit._cdf_tk.utils.collection import (
    chunker_sequence,
    flatten_dict_json_path,
    get_string_nested_dict,
    set_nested_dict_if_path_exists,
)


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
                    resource_type="timeseries",
                    id=1,
                    data_set_id=123,
                    instance_id=NodeId(space="sp_full_ts", external_id="full_ts_id"),
                ),
                MigrationMapping(
                    resource_type="timeseries",
                    id=2,
                    data_set_id=None,
                    instance_id=NodeId(space="sp_step_ts", external_id="step_ts_id"),
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
            pytest.param(
                {
                    "name": "MyAsset",
                    "metadata": {"雪ヘ罪約べげド. [10] SNL": "値テスト"},
                },
                set(),
                {
                    "name": "MyAsset",
                    "metadata.雪ヘ罪約べげド. [10] SNL": "値テスト",
                },
                id="Unicode characters in keys",
            ),
        ],
    )
    def test_flatten_dict_json_path(self, dct: dict[str, Any], exclude_keys: Set[str], expected: dict[str, Any]):
        """Test that flatten_dict_json_path correctly flattens a nested dictionary."""
        flat_dict = flatten_dict_json_path(dct, keep_structured=exclude_keys)
        assert flat_dict == expected


class TestGetStringNestedDict:
    @pytest.mark.parametrize(
        "dct, path, expected",
        [
            pytest.param({"a": {"b": {"c": "value"}}}, ("a", "b", "c"), "value", id="simple nested dict"),
            pytest.param({"a": [{"b": {"c": "value"}}]}, ("a", 0, "b", "c"), "value", id="nested dict in list"),
            pytest.param({"a": {"b": {"c": "value"}}}, ("x", "y", "z"), None, id="non-existent path"),
            pytest.param({}, ("a", "b", "c"), None, id="empty dict"),
        ],
    )
    def test_get_string_nested_dict(self, dct: dict[str, Any], path: tuple[str, ...], expected: str | None):
        """Test that get_string_nested_dict correctly retrieves a value from a nested dictionary."""
        assert get_string_nested_dict(dct, path) == expected

    @pytest.mark.parametrize(
        "dct, path",
        [
            pytest.param({"a": {"b": "not a dict"}}, ("a", "b", "c"), id="non-dict intermediate value"),
            pytest.param({"a": {"b": {"value"}}}, ("a", 0, "b"), id="non-list intermediate value"),
            pytest.param({"a": {"b": 123}}, ("a", "b"), id="non-string value"),
        ],
    )
    def test_get_string_nested_dict_raises(self, dct: dict[str, Any], path: tuple[str, ...]):
        """Test that get_string_nested_dict raises TypeError for non-dict intermediate values."""
        with pytest.raises(TypeError):
            get_string_nested_dict(dct, path)


class TestSetNestedDictIfPathExists:
    @pytest.mark.parametrize(
        "dct, path, value, expected",
        [
            pytest.param({"a": {"b": {"c": 1}}}, ("a", "b", "c"), 2, {"a": {"b": {"c": 2}}}, id="simple nested dict"),
            pytest.param(
                {"a": [{"b": {"c": 1}}]}, ("a", 0, "b", "c"), 2, {"a": [{"b": {"c": 2}}]}, id="nested dict in list"
            ),
        ],
    )
    def test_set_nested_dict_if_path_exists(
        self, dct: dict[str, Any], path: tuple[str | int, ...], value: Any, expected: dict[str, Any]
    ):
        """Test that set_nested_dict_if_path_exists correctly sets a value in a nested dictionary."""
        assert dct != expected  # Ensure the original dict is not already equal to expected
        set_nested_dict_if_path_exists(dct, path, value)
        assert dct == expected

    @pytest.mark.parametrize(
        "dct, path, value",
        [
            pytest.param({"a": {"b": {"c": 1}}}, ("x", "y", "z"), 2, id="non-existent path"),
            pytest.param({}, ("a", "b", "c"), 2, id="empty dict"),
        ],
    )
    def test_set_nested_dict_if_path_exists_no_change(
        self, dct: dict[str, Any], path: tuple[str | int, ...], value: Any
    ):
        """Test that set_nested_dict_if_path_exists does not change the dict if the path does not exist."""
        original_dct = dct.copy()
        set_nested_dict_if_path_exists(dct, path, value)
        assert dct == original_dct  # The dict should remain unchanged

    @pytest.mark.parametrize(
        "dct, path",
        [
            pytest.param({"a": {"b": "not a dict"}}, ("a", "b", "c"), id="non-dict intermediate value"),
            pytest.param({"a": [{"b": {"c": 1}}]}, ("a", 0, "b", 0), id="non-list intermediate value"),
        ],
    )
    def test_set_nested_dict_if_path_exists_raises(self, dct: dict[str, Any], path: tuple[str | int, ...]):
        """Test that set_nested_dict_if_path_exists raises TypeError for non-dict or non-list intermediate values."""
        with pytest.raises(TypeError):
            set_nested_dict_if_path_exists(dct, path, value=42)
