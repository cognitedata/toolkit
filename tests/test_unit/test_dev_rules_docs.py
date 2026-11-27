"""Tests for the dev.py command generating the rules library documentation."""

import dev
from cognite_toolkit._cdf_tk.data_classes._insights import RuleDefinition

_CYCLE = RuleDefinition(
    code="INVALID-VALUE",
    resource_types=("View",),
    severity="Error",
    context="Build",
    alpha=True,
    reported_when="Views form a cycle.",
)
_REJECTED = RuleDefinition(
    code="INVALID-VALUE",
    resource_types=("Function", "InField location config"),
    severity="Warning",
    context="Deploy",
    reported_when="CDF rejects a value.",
)
_MISSING = RuleDefinition(
    code="MISSING-VALUE",
    resource_types=("Container",),
    severity="Warning",
    context="Build",
    reported_when="A value is missing.",
)


def test_replace_mdx_tables_keeps_prose_and_reports_mismatched_sections() -> None:
    content = "\n".join(
        [
            "### INVALID-VALUE {#invalid-value}",
            "",
            "A value isn't valid.",
            "",
            "| Resource type | Severity |",
            "|---------------|----------|",
            "| Old           | Error    |",
            "",
            "To fix it, use a valid value.",
            "",
            "### UNUSED-RESOURCE {#unused-resource}",
            "",
            "| Resource type |",
            "|---------------|",
            "| Space         |",
        ]
    )

    result = dev.replace_mdx_tables(content, [_CYCLE, _REJECTED, _MISSING])

    assert result == (
        "\n".join(
            [
                "### INVALID-VALUE {#invalid-value}",
                "",
                "A value isn't valid.",
                "",
                "| Resource type                     | Severity | Context | Alpha | Reported when        |",
                "|-----------------------------------|----------|---------|-------|----------------------|",
                "| View                              | Error    | Build   | Yes   | Views form a cycle.  |",
                "| Function, InField location config | Warning  | Deploy  | No    | CDF rejects a value. |",
                "",
                "To fix it, use a valid value.",
                "",
                "### UNUSED-RESOURCE {#unused-resource}",
                "",
                "| Resource type |",
                "|---------------|",
                "| Space         |",
            ]
        ),
        ["MISSING-VALUE"],
        ["UNUSED-RESOURCE"],
    )
