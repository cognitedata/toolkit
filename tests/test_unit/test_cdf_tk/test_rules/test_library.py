import inspect
import re
import sys

import pytest

from cognite_toolkit._cdf_tk.rules import ToolkitRuleSet, get_global_rules_registry, get_rules_library

_DIRECT_INSIGHT = re.compile(r"\b(?:BuildError|BuildWarning|Recommendation)\(")


@pytest.mark.parametrize(
    "rule_set",
    [rule_set for rule_set in get_global_rules_registry() if rule_set.RULES],
    ids=lambda rule_set: rule_set.__name__,
)
def test_rule_set_with_rules_creates_insights_from_them(rule_set: type[ToolkitRuleSet]) -> None:
    # Insights created directly bypass the rule definitions, so the rules library documentation would miss them.
    assert _DIRECT_INSIGHT.findall(inspect.getsource(sys.modules[rule_set.__module__])) == []


def test_rules_library_has_no_duplicates() -> None:
    rules = get_rules_library()

    assert {rule for rule in rules if rules.count(rule) > 1} == set()
