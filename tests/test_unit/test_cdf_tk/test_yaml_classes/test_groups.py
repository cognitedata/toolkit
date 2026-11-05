from collections.abc import Iterable
from pathlib import Path

import pytest
from pydantic import TypeAdapter
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning, InsightList, Recommendation
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import GroupIO
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_content
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes import GroupYAML
from tests.test_unit.utils import find_resources


def invalid_group_test_cases() -> Iterable:
    yield pytest.param(
        """name: gp_table_scoped_complete_org
sourceId: '1234567890123456789'
metadata:
  origin: cognite-toolkit
  governed: true
  groupNo: 0123
  maxUsers: .inf
  description: foo:bar
capabilities:
- rawAcl:
    actions:
    - READ
    - WRITE
    scope:
      tableScope:
        dbsToTables:
          db_complete_org: {}
""",
        {
            "Invalid value at capabilities[1].scope.dbsToTables.db_complete_org: Input should be a valid list. Got {}.",
        },
        id="Naughty metadata and non-standard rawAcl tableScope",
    )

    yield pytest.param(
        """- name: group1
  sourceId: '1234567890123456789'
  capabilities:
  - labelsAcl:
     actions:
     - READ
     scope:
       all: {}
- name: group2
  sourceId: '1234567890123456789'
  capabilities:
  - labelsAcl:
     actions:
     - WRITE-KING
     scope:
       all: {}
""",
        {
            "Unrecognized value for item [2].capabilities[1].actions[1]: Expected one of 'READ' or 'WRITE'. "
            "Got 'WRITE-KING'."
        },
        id="Error in second group",
    )
    yield pytest.param(
        """name: group1
sourceId: '1234567890123456789'
members: allUserAccounts
""",
        {"Invalid group definition: Cannot have both 'sourceId' and 'members'. Please specify only one."},
        id="Both sourceId and members present",
    )


@pytest.fixture(scope="session")
def group_adapter() -> TypeAdapter[GroupYAML]:
    return TypeAdapter(GroupYAML)


class TestGroupYAML:
    @pytest.mark.parametrize("data", list(find_resources("Group")))
    def test_load_valid_groups(self, data: dict[str, object], group_adapter: TypeAdapter[GroupYAML]) -> None:
        loaded = group_adapter.validate_python(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_load_group_with_attributes(self, group_adapter: TypeAdapter[GroupYAML]) -> None:
        data = {
            "name": "group-with-app-ids",
            "sourceId": "1234567890123456789",
            "attributes": {
                "token": {
                    "appIds": ["my-app-id"],
                },
            },
        }
        loaded = group_adapter.validate_python(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("content, expected_errors", list(invalid_group_test_cases()))
    def test_invalid_group_error_messages(
        self, content: str, expected_errors: set[str], group_adapter: TypeAdapter[GroupYAML]
    ) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        data = read_yaml_content(content)

        warning_list = validate_resource_yaml_pydantic(data, GroupYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors


def _group_insights(capabilities: list[dict[str, object]]) -> list[BuildWarning | Recommendation]:
    loaded = TypeAdapter(GroupYAML).validate_python(
        {"name": "readers", "sourceId": "abc", "capabilities": capabilities}
    )
    return loaded.extra_insights(Path("group.Group.yaml"), GroupIO.as_resource_type())


def _insight_view(insights: list[BuildWarning | Recommendation]) -> list[tuple[type, str, bool, str, str | None]]:
    return [(type(insight), insight.code, insight.alpha, insight.message, insight.fix) for insight in insights]


class TestGroupRedundantCapabilities:
    def test_space_scope_covered_by_all(self) -> None:
        insights = _group_insights(
            [
                {
                    "dataModelInstancesAcl": {
                        "actions": ["READ", "WRITE"],
                        "scope": {"spaceIdScope": {"spaceIds": ["my-space"]}},
                    }
                },
                {
                    "dataModelInstancesAcl": {
                        "actions": ["READ", "WRITE"],
                        "scope": {"all": {}},
                    }
                },
            ]
        )

        assert _insight_view(insights) == [
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has a dataModelInstancesAcl with READ and WRITE, "
                "scoped to spaceIdScope (spaceIds: my-space), that is already covered by the group's other "
                "dataModelInstancesAcl entries.",
                "Remove this dataModelInstancesAcl.",
            )
        ]

    def test_partial_action_keeps_the_acl(self) -> None:
        insights = _group_insights(
            [
                {
                    "dataModelInstancesAcl": {
                        "actions": ["READ", "WRITE"],
                        "scope": {"spaceIdScope": {"spaceIds": ["my-space"]}},
                    }
                },
                {"dataModelInstancesAcl": {"actions": ["READ"], "scope": {"all": {}}}},
            ]
        )

        assert _insight_view(insights) == [
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has a dataModelInstancesAcl whose READ action, "
                "scoped to spaceIdScope (spaceIds: my-space), is already covered by the group's other "
                "dataModelInstancesAcl entries.",
                "Remove the READ action from this dataModelInstancesAcl.",
            )
        ]

    def test_different_actions_are_kept(self) -> None:
        insights = _group_insights(
            [
                {
                    "dataModelInstancesAcl": {
                        "actions": ["WRITE"],
                        "scope": {"spaceIdScope": {"spaceIds": ["my-space"]}},
                    }
                },
                {"dataModelInstancesAcl": {"actions": ["READ"], "scope": {"all": {}}}},
            ]
        )

        assert insights == []

    def test_exact_duplicate_keeps_the_first(self) -> None:
        insights = _group_insights(
            [
                {"assetsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
                {"assetsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
                {"assetsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
            ]
        )

        assert _insight_view(insights) == [
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has an assetsAcl with READ, scoped to all, "
                "that is already covered by the group's other assetsAcl entries.",
                "Remove this assetsAcl.",
            ),
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has an assetsAcl with READ, scoped to all, "
                "that is already covered by the group's other assetsAcl entries.",
                "Remove this assetsAcl.",
            ),
        ]

    def test_narrower_dataset_scope_is_covered(self) -> None:
        insights = _group_insights(
            [
                {"assetsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["ds_files", "ds_events"]}}}},
                {"assetsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["ds_files"]}}}},
            ]
        )

        assert _insight_view(insights) == [
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has an assetsAcl with READ, scoped to datasetScope (ids: ds_files), "
                "that is already covered by the group's other assetsAcl entries.",
                "Remove this assetsAcl.",
            )
        ]

    def test_combined_dataset_scope_covered_by_union(self) -> None:
        insights = _group_insights(
            [
                {"assetsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["ds_files"]}}}},
                {"assetsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["ds_events"]}}}},
                {
                    "assetsAcl": {
                        "actions": ["READ"],
                        "scope": {"datasetScope": {"ids": ["ds_files", "ds_events"]}},
                    }
                },
            ]
        )

        assert _insight_view(insights) == [
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has an assetsAcl with READ, scoped to datasetScope (ids: ds_files, ds_events), "
                "that is already covered by the group's other assetsAcl entries.",
                "Remove this assetsAcl.",
            )
        ]

    def test_table_scope_subset_is_covered(self) -> None:
        insights = _group_insights(
            [
                {
                    "rawAcl": {
                        "actions": ["READ"],
                        "scope": {"tableScope": {"dbsToTables": {"my_db": ["table_a", "table_b"]}}},
                    }
                },
                {
                    "rawAcl": {
                        "actions": ["READ"],
                        "scope": {"tableScope": {"dbsToTables": {"my_db": ["table_a"]}}},
                    }
                },
            ]
        )

        assert _insight_view(insights) == [
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                True,
                "Group 'readers' has a rawAcl with READ, scoped to tableScope (dbsToTables: my_db: table_a), "
                "that is already covered by the group's other rawAcl entries.",
                "Remove this rawAcl.",
            )
        ]

    def test_table_scope_without_tables_covers_the_whole_database(self) -> None:
        insights = _group_insights(
            [
                {"rawAcl": {"actions": ["READ"], "scope": {"tableScope": {"dbsToTables": {"my_db": []}}}}},
                {"rawAcl": {"actions": ["READ"], "scope": {"tableScope": {"dbsToTables": {"my_db": ["table_a"]}}}}},
            ]
        )

        assert [insight.message for insight in insights] == [
            "Group 'readers' has a rawAcl with READ, scoped to tableScope (dbsToTables: my_db: table_a), "
            "that is already covered by the group's other rawAcl entries."
        ]

    def test_different_scope_kinds_do_not_cover_each_other(self) -> None:
        insights = _group_insights(
            [
                {"timeSeriesAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["ds_files"]}}}},
                {"timeSeriesAcl": {"actions": ["READ"], "scope": {"idscope": {"ids": ["ts_temperature"]}}}},
            ]
        )

        assert insights == []

    def test_unknown_capabilities_are_skipped(self) -> None:
        insights = _group_insights(
            [
                {"notARealAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
                {
                    "dataModelInstancesAcl": {
                        "actions": ["READ", "WRITE"],
                        "scope": {"spaceIdScope": {"spaceIds": ["my-space"]}},
                    }
                },
                {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
                {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
            ]
        )

        assert [(type(insight), insight.code, insight.message) for insight in insights] == [
            (
                BuildWarning,
                "UNRECOGNIZED-VALUE",
                "'notARealAcl' is not a known capability name. It will be deployed as-is, but may be rejected by CDF.",
            ),
            (
                Recommendation,
                "REDUNDANT-PROPERTY",
                "Group 'readers' has a labelsAcl with READ, scoped to all, "
                "that is already covered by the group's other labelsAcl entries.",
            ),
        ]

    def test_reported_when_alpha_rules_enabled(self, monkeypatch: MonkeyPatch) -> None:
        insights = _group_insights(
            [
                {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
                {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
            ]
        )
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)

        reported = BuildV2Command._filter_to_reported_insights(InsightList(insights), {}, set())

        assert list(reported) == insights

    def test_hidden_when_alpha_rules_disabled(self, monkeypatch: MonkeyPatch) -> None:
        insights = _group_insights(
            [
                {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
                {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
            ]
        )
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: False)

        hidden = BuildV2Command._filter_to_reported_insights(InsightList(insights), {}, set())

        assert list(hidden) == []
