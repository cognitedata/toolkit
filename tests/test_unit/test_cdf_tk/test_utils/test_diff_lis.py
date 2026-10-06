from typing import Any

import pytest

from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_identifiable, hash_dict


class TestDiffListHashable:
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
