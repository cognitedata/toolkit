from typing import Any

import pytest

from cognite_toolkit._cdf_tk.utils.diff_list import (
    diff_list_force_hashable,
    diff_list_hashable,
    diff_list_identifiable,
    dm_identifier,
    force_hash,
    hash_dict,
    hash_list,
)


class TestDiffListPrimitives:
    @pytest.mark.parametrize(
        "local, cdf, expected_local_by_cdf, expected_added",
        [
            pytest.param([], [], {}, [], id="Both empty"),
            pytest.param([], ["a", "b"], {}, [0, 1], id="Local empty"),
            pytest.param(["a", "b"], [], {}, [], id="CDF empty"),
            pytest.param(["a", "b", "c"], ["c", "a", "b"], {0: 1, 1: 2, 2: 0}, [], id="Reordered"),
            pytest.param([1, 2], [2, 3], {1: 0}, [1], id="Partial overlap ints"),
            pytest.param([None, "x"], ["x", None], {0: 1, 1: 0}, [], id="None is a valid item"),
            pytest.param([("a", 1)], [("a", 1), ("a", 2)], {0: 0}, [1], id="Tuples"),
            # Regression: duplicates used to overwrite each other.
            pytest.param(["a"], ["a", "a"], {0: 0}, [1], id="Duplicate in CDF is added"),
            pytest.param(["a", "a"], ["a"], {0: 0}, [], id="Duplicate in local, first is matched"),
            pytest.param(["a", "a"], ["a", "a"], {0: 0, 1: 1}, [], id="Duplicates in both matched in order"),
            pytest.param(["a", "b", "a"], ["b", "a", "a", "a"], {0: 1, 1: 0, 2: 2}, [3], id="Mixed duplicates"),
        ],
    )
    def test_diff_list_hashable(
        self, local: list, cdf: list, expected_local_by_cdf: dict[int, int], expected_added: list[int]
    ) -> None:
        assert diff_list_hashable(local, cdf) == (expected_local_by_cdf, expected_added)

    @pytest.mark.parametrize(
        "local, cdf, expected_local_by_cdf, expected_added",
        [
            pytest.param([1, "a"], ["a", 1], {0: 1, 1: 0}, [], id="Primitives"),
            pytest.param([[1, 2]], [[2, 1]], {}, [0], id="List order matters"),
            pytest.param([{}], [[]], {}, [0], id="Empty dict is not empty list"),
            pytest.param([True], [1], {}, [0], id="Bool is not int"),
            pytest.param([{"a": 1}], [{"a": 1}, {"a": 1}], {0: 0}, [1], id="Duplicate dict in CDF is added"),
            pytest.param(
                [{"a": [1, 2]}, [{"b": 1}]],
                [[{"b": 1}], {"a": [1, 2]}],
                {0: 1, 1: 0},
                [],
                id="Mixed dicts and lists",
            ),
        ],
    )
    def test_diff_list_force_hashable(
        self, local: list, cdf: list, expected_local_by_cdf: dict[int, int], expected_added: list[int]
    ) -> None:
        assert diff_list_force_hashable(local, cdf) == (expected_local_by_cdf, expected_added)


class TestHashDict:
    @pytest.mark.parametrize(
        "a, b",
        [
            pytest.param({"a": 1, "b": 2}, {"b": 2, "a": 1}, id="Key order"),
            pytest.param({"x": {"a": 1, "b": 2}}, {"x": {"b": 2, "a": 1}}, id="Nested key order"),
            pytest.param({}, {}, id="Empty"),
            pytest.param({"a": [{"b": 1, "c": 2}]}, {"a": [{"c": 2, "b": 1}]}, id="Dict in list key order"),
        ],
    )
    def test_equal(self, a: dict, b: dict) -> None:
        assert hash_dict(a) == hash_dict(b)

    @pytest.mark.parametrize(
        "a, b",
        [
            pytest.param({"a": 1}, {"a": 2}, id="Different values"),
            pytest.param({"a": 1}, {"b": 1}, id="Different keys"),
            pytest.param({"a": "b"}, {"b": "a"}, id="Key and value swapped"),
            pytest.param({"a": 1, "b": 2}, {"a": 2, "b": 1}, id="Values swapped between keys"),
            # Regression: nested containers used to ignore the parent key.
            pytest.param(
                {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                id="Nested dict under different keys",
            ),
            pytest.param({"a": [1, 2]}, {"b": [1, 2]}, id="Nested list under different keys"),
            # Regression: empty containers used to hash to 0.
            pytest.param({"a": {}}, {}, id="Empty nested dict vs empty"),
            pytest.param({"a": []}, {}, id="Empty nested list vs empty"),
            pytest.param({"a": {}}, {"a": []}, id="Empty dict vs empty list"),
            pytest.param({"scope": {"all": {}}}, {"scope": {"none": {}}}, id="Different empty-valued keys"),
            # Regression: XOR made duplicates cancel out.
            pytest.param({"a": {"x": 1}, "b": {"x": 1}}, {}, id="Duplicate nested values cancel"),
            pytest.param({"a": None}, {}, id="None value vs missing key"),
            pytest.param({"a": None}, {"a": {}}, id="None vs empty dict"),
            pytest.param({"a": "1"}, {"a": 1}, id="String vs int"),
            pytest.param({"a": True}, {"a": 1}, id="True vs 1"),
            pytest.param({"a": False}, {"a": 0}, id="False vs 0"),
            pytest.param({"a": {"b": 1}}, {"a": [{"b": 1}]}, id="Dict vs list containing dict"),
            pytest.param({"a": {"b": {"c": 1}}}, {"a": {"c": {"b": 1}}}, id="Nested keys swapped"),
        ],
    )
    def test_not_equal(self, a: dict, b: dict) -> None:
        assert hash_dict(a) != hash_dict(b)

    def test_unhashable_value_raises(self) -> None:
        with pytest.raises(ValueError):
            hash_dict({"a": {1, 2}})


class TestHashList:
    @pytest.mark.parametrize(
        "a, b",
        [
            pytest.param([1, 2], [2, 1], id="Order matters"),
            # Regression: XOR made duplicates cancel out.
            pytest.param([1, 1], [], id="Duplicates do not cancel"),
            pytest.param([1, 1, 2], [2], id="Duplicates do not cancel with other items"),
            pytest.param([{"a": 1}, {"a": 1}], [], id="Duplicate dicts do not cancel"),
            # Regression: dicts inside lists used to lose their position.
            pytest.param([{"a": 1}, {"b": 2}], [{"b": 2}, {"a": 1}], id="Dict items order matters"),
            pytest.param([[1], [2]], [[2], [1]], id="Nested list items order matters"),
            pytest.param([[]], [], id="Empty nested list vs empty"),
            pytest.param([{}], [], id="Empty nested dict vs empty"),
            pytest.param([{}], [[]], id="Empty dict vs empty list item"),
            pytest.param([[1, 2]], [1, 2], id="Nesting matters"),
            pytest.param([None], [], id="None item vs empty"),
            pytest.param([True], [1], id="Bool item vs int"),
        ],
    )
    def test_not_equal(self, a: list, b: list) -> None:
        assert hash_list(a) != hash_list(b)

    def test_equal(self) -> None:
        assert hash_list([{"a": 1, "b": [1, 2]}]) == hash_list([{"b": [1, 2], "a": 1}])

    def test_unhashable_item_raises(self) -> None:
        with pytest.raises(ValueError):
            hash_list([{1, 2}])


class TestForceHash:
    @pytest.mark.parametrize("value", [1, "a", None, (1, 2), 1.5])
    def test_hashable(self, value: Any) -> None:
        assert force_hash(value) == hash(value)

    def test_dict(self) -> None:
        assert force_hash({"a": [1]}) == hash_dict({"a": [1]})

    def test_list(self) -> None:
        assert force_hash([{"a": 1}]) == hash_list([{"a": 1}])

    def test_empty_dict_vs_empty_list(self) -> None:
        assert force_hash({}) != force_hash([])

    def test_bool_vs_int(self) -> None:
        assert force_hash(True) != force_hash(1)

    def test_tuple_with_unhashable_elements(self) -> None:
        assert force_hash((1, [2])) == force_hash((1, [2]))
        assert force_hash((1, [2])) != force_hash((1, [3]))

    @pytest.mark.parametrize("value", [{1, 2}, bytearray(b"a")])
    def test_unhashable_raises(self, value: Any) -> None:
        with pytest.raises(ValueError):
            force_hash(value)


class TestDMIdentifier:
    @pytest.mark.parametrize(
        "data, expected",
        [
            pytest.param({"space": "s", "externalId": "x"}, ("", "s", "x", ""), id="Minimal"),
            pytest.param(
                {"type": "view", "space": "s", "externalId": "x", "version": "v1"},
                ("view", "s", "x", "v1"),
                id="All fields",
            ),
            pytest.param({"space": "s", "externalId": "x", "name": "ignored"}, ("", "s", "x", ""), id="Extra ignored"),
        ],
    )
    def test_dm_identifier(self, data: dict[str, Any], expected: tuple[str, ...]) -> None:
        assert dm_identifier(data) == expected

    def test_version_distinguishes(self) -> None:
        a = {"space": "s", "externalId": "x", "version": "v1"}
        b = {"space": "s", "externalId": "x", "version": "v2"}
        assert dm_identifier(a) != dm_identifier(b)

    @pytest.mark.parametrize("data", [{"externalId": "x"}, {"space": "s"}])
    def test_missing_required_raises(self, data: dict[str, Any]) -> None:
        with pytest.raises(KeyError):
            dm_identifier(data)


class TestDiffListIdentifiable:
    @pytest.mark.parametrize(
        "local, cdf, expected_local_by_cdf, expected_added",
        [
            pytest.param([], [], {}, [], id="Both empty"),
            pytest.param(
                [{"a": 1}, {"b": 2}],
                [{"a": 1}, {"b": 2}],
                {0: 0, 1: 1},
                [],
                id="Identical lists",
            ),
            pytest.param(
                [{"a": 1}, {"b": 2}],
                [{"b": 2}, {"a": 1}],
                {0: 1, 1: 0},
                [],
                id="Same items, different order",
            ),
            pytest.param(
                [{"a": 1}],
                [{"a": 1}, {"c": 3}],
                {0: 0},
                [1],
                id="Extra item in CDF",
            ),
            pytest.param(
                [{"a": 1}, {"b": 2}],
                [{"a": 1}],
                {0: 0},
                [],
                id="Extra item in local",
            ),
            pytest.param(
                [{"a": 1, "b": 2}],
                [{"b": 2, "a": 1}],
                {0: 0},
                [],
                id="Dict key order does not matter",
            ),
            pytest.param(
                [{"a": {"nested": [1, 2]}}],
                [{"a": {"nested": [2, 1]}}],
                {},
                [0],
                id="Nested list order matters",
            ),
            pytest.param(
                [{"a": 1}],
                [{"a": 2}],
                {},
                [0],
                id="Different values",
            ),
            pytest.param(
                [
                    {"projectsAcl": {"actions": ["LIST", "READ"], "scope": {"all": {}}}},
                    {"groupsAcl": {"actions": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"], "scope": {"all": {}}}},
                    {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"rawAcl": {"actions": ["READ", "WRITE", "LIST"], "scope": {"all": {}}}},
                    {"timeSeriesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"dataModelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"dataModelInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"all": {}}}},
                    {"extractionPipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"extractionRunsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"extractionConfigsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"functionsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"sessionsAcl": {"actions": ["LIST", "CREATE", "DELETE"], "scope": {"all": {}}}},
                    {"transformationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"annotationsAcl": {"actions": ["WRITE"], "scope": {"all": {}}}},
                ],
                [
                    {"projectsAcl": {"actions": ["LIST", "READ"], "scope": {"all": {}}}},
                    {"groupsAcl": {"actions": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"], "scope": {"all": {}}}},
                    {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"rawAcl": {"actions": ["READ", "WRITE", "LIST"], "scope": {"all": {}}}},
                    {"timeSeriesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"dataModelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"dataModelInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"all": {}}}},
                    {"extractionPipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"extractionRunsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"extractionConfigsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"functionsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"sessionsAcl": {"actions": ["LIST", "CREATE", "DELETE"], "scope": {"all": {}}}},
                    {"transformationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"annotationsAcl": {"actions": ["WRITE"], "scope": {"all": {}}}},
                    {"eventsAcl": {"actions": ["WRITE", "READ"], "scope": {"all": {}}}},
                    {"labelsAcl": {"actions": ["WRITE", "READ"], "scope": {"all": {}}}},
                    {"entitymatchingAcl": {"actions": ["WRITE", "READ"], "scope": {"all": {}}}},
                    {"sequencesAcl": {"actions": ["READ"], "scope": {"all": {}}}},
                    {"timeSeriesSubscriptionsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"locationFiltersAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                    {"workflowOrchestrationAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                ],
                {i: i for i in range(16)},
                list(range(16, 23)),
                id="Real world example with many items, some added in CDF",
            ),
        ],
    )
    def test_diff_list_identifiable(
        self,
        local: list[dict[str, Any]],
        cdf: list[dict[str, Any]],
        expected_local_by_cdf: dict[int, int],
        expected_added: list[int],
    ) -> None:
        local_by_cdf, added = diff_list_identifiable(local, cdf, get_identifier=hash_dict)

        assert local_by_cdf == expected_local_by_cdf
        assert added == expected_added
