from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import NameId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightDefinition
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import GroupResourceScopedIO
from cognite_toolkit._cdf_tk.rules._groups import GroupRuleSet

ALL_SCOPED_GROUP = """name: all_scoped
sourceId: '123'
capabilities:
  - dataModelsAcl:
      actions: [READ]
      scope:
        all: {}
  - assetsAcl:
      actions: [READ]
      scope:
        all: {}
"""

RESOURCE_SCOPED_GROUP = """name: resource_scoped
sourceId: '456'
capabilities:
  - eventsAcl:
      actions: [READ]
      scope:
        datasetScope:
          ids: [my_dataset]
"""


def _group_resource(tmp_path: Path, name: str, content: str) -> BuiltResource:
    path = tmp_path / f"{name}.Group.yaml"
    path.write_text(content)
    return BuiltResource(
        identifier=NameId(name=name),
        source_hash="test-hash",
        type=GroupResourceScopedIO.as_resource_type(),
        source_path=path.resolve(),
        build_path=path.resolve(),
        crud_cls=GroupResourceScopedIO,
        has_syntax_error=False,
        module_id=ModuleId(id=Path("modules/my"), path=tmp_path.resolve()),
    )


def _client(data_modeling_status: str) -> MagicMock:
    client = MagicMock()
    client.project.status.return_value.this_project.data_modeling_status = data_modeling_status
    client.lookup.data_sets.id.return_value = 789
    return client


@pytest.fixture
def alpha_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)


class TestGroupRuleSet:
    def test_skipped_without_alpha_rules(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: False)
        client = _client("DATA_MODELING_ONLY")
        rule = GroupRuleSet(resources=[_group_resource(tmp_path, "all_scoped", ALL_SCOPED_GROUP)], client=client)

        assert (rule.get_status().code, list(rule.validate())) == ("skip", [])

    @pytest.mark.usefixtures("alpha_rules")
    @pytest.mark.parametrize(
        "data_modeling_status, expected_type",
        [
            pytest.param("DATA_MODELING_ONLY", "Warning", id="data-modeling-only"),
            pytest.param("HYBRID", "Recommendation", id="hybrid"),
        ],
    )
    def test_deprecated_acl(self, tmp_path: Path, data_modeling_status: str, expected_type: str) -> None:
        resources = [
            _group_resource(tmp_path, "all_scoped", ALL_SCOPED_GROUP),
            _group_resource(tmp_path, "resource_scoped", RESOURCE_SCOPED_GROUP),
        ]
        rule = GroupRuleSet(resources=resources, client=_client(data_modeling_status))

        results = list(rule.validate())
        insights = [item for item in results if isinstance(item, InsightDefinition)]

        assert [(item.insight_type, item.code, item.source_file, item.line, item.alpha) for item in insights] == [
            (expected_type, "DEPRECATED-FEATURE", resources[0].source_path, 8, True),
            (expected_type, "DEPRECATED-FEATURE", resources[1].source_path, 4, True),
        ]

    @pytest.mark.usefixtures("alpha_rules")
    def test_no_project_status_call_without_deprecated_acls(self, tmp_path: Path) -> None:
        content = ALL_SCOPED_GROUP.split("  - assetsAcl")[0]
        client = _client("DATA_MODELING_ONLY")
        rule = GroupRuleSet(resources=[_group_resource(tmp_path, "all_scoped", content)], client=client)

        assert (list(rule.validate()), client.project.status.called) == ([], False)
