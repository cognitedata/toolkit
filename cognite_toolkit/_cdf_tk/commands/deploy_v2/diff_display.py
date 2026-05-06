"""Human-readable deploy drift views (YAML vs CDF API)."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from difflib import SequenceMatcher
from enum import Enum
from pathlib import Path
from typing import Any

from rich.console import Group
from rich.text import Text

from cognite_toolkit._cdf_tk.ui import AuraColor, ToolkitPanel, ToolkitPanelSection, ToolkitTable
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


class DeployDiffFormat(str, Enum):
    human = "human"


# Rich Padding (top, right, bottom, left): one consistent body inset for every diff block.
_SECTION_BODY_PADDING = (0, 0, 1, 2)


def _sanitize(text: str, sensitive_strings: Iterable[str]) -> str:
    for sensitive in sensitive_strings:
        text = text.replace(sensitive, "********")
    return text


def _summarize_opcodes(opcodes: Sequence[tuple[str, int, int, int, int]]) -> tuple[int, int, int, int]:
    """Returns counts of: delete-only lines, insert-only lines, replace blocks, equal lines (excluding collapsed)."""
    delete_lines = 0
    insert_lines = 0
    replace_blocks = 0
    equal_lines = 0
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "delete":
            delete_lines += i2 - i1
        elif tag == "insert":
            insert_lines += j2 - j1
        elif tag == "replace":
            replace_blocks += 1
        elif tag == "equal":
            equal_lines += i2 - i1
    return delete_lines, insert_lines, replace_blocks, equal_lines


def _side_by_side_rows(
    cdf_lines: list[str],
    yaml_lines: list[str],
    *,
    equal_context: int = 3,
    equal_collapse_at: int = 12,
) -> Iterator[tuple[Text, Text]]:
    """Yield (CDF column cell, local build column cell); the table swaps them to show build left."""
    matcher = SequenceMatcher(None, cdf_lines, yaml_lines)
    opcodes = matcher.get_opcodes()

    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            span = i2 - i1
            if span <= equal_collapse_at:
                for offset in range(span):
                    line_l = cdf_lines[i1 + offset]
                    line_r = yaml_lines[j1 + offset]
                    yield (Text(line_l, style="dim"), Text(line_r, style="dim"))
            else:
                head = equal_context
                tail = equal_context
                for offset in range(head):
                    yield (
                        Text(cdf_lines[i1 + offset], style="dim"),
                        Text(yaml_lines[j1 + offset], style="dim"),
                    )
                omitted = span - head - tail
                yield (
                    Text(f"... {omitted} unchanged line(s) ...", style="italic dim"),
                    Text(f"... {omitted} unchanged line(s) ...", style="italic dim"),
                )
                for offset in range(span - tail, span):
                    yield (
                        Text(cdf_lines[i1 + offset], style="dim"),
                        Text(yaml_lines[j1 + offset], style="dim"),
                    )
        elif tag == "delete":
            for idx in range(i1, i2):
                yield (Text(cdf_lines[idx], style="red"), Text("-", style="dim"))
        elif tag == "insert":
            for idx in range(j1, j2):
                yield (Text("-", style="dim"), Text(yaml_lines[idx], style="green"))
        elif tag == "replace":
            left = cdf_lines[i1:i2]
            right = yaml_lines[j1:j2]
            for k in range(max(len(left), len(right))):
                l_txt = left[k] if k < len(left) else ""
                r_txt = right[k] if k < len(right) else ""
                yield (Text(l_txt, style="yellow"), Text(r_txt, style="cyan"))


def _build_side_by_side_table(cdf_lines: list[str], yaml_lines: list[str], *, cdf_project: str) -> ToolkitTable:
    # Local build left, CDF (project) right — row cells are swapped from matcher order (CDF, build).
    table = ToolkitTable("Local build", f"CDF ({cdf_project})", expand=True, padding=(0, 0))
    table.columns[0].overflow = "fold"
    table.columns[1].overflow = "fold"
    table.columns[0].justify = "left"
    table.columns[1].justify = "left"
    for cdf_cell, build_cell in _side_by_side_rows(cdf_lines, yaml_lines):
        table.add_row(build_cell, cdf_cell)
    return table


def render_deploy_human_diff(
    *,
    resource_name: str,
    identifier: Any,
    source_file: Path,
    cdf_dict: dict[str, Any],
    yaml_dict: dict[str, Any],
    sensitive_strings: Iterable[str],
    cdf_project: str,
) -> ToolkitPanel:
    sens = list(sensitive_strings)
    cdf_yaml = _sanitize(yaml_safe_dump(cdf_dict, sort_keys=True), sens)
    build_yaml = _sanitize(yaml_safe_dump(yaml_dict, sort_keys=True), sens)
    cdf_lines = cdf_yaml.splitlines()
    yaml_lines = build_yaml.splitlines()

    matcher = SequenceMatcher(None, cdf_lines, yaml_lines)
    delete_lines, insert_lines, replace_blocks, equal_lines = _summarize_opcodes(matcher.get_opcodes())

    summary_lines = [
        f"Serialized YAML: {len(cdf_lines)} line(s) from CDF vs {len(yaml_lines)} line(s) from build",
        f"{equal_lines} unchanged line(s)",
        f"{replace_blocks} replaced region(s)",
        f"{delete_lines} line(s) present in {cdf_project} only",
        f"{insert_lines} line(s) present in build only",
    ]

    sections = [
        ToolkitPanelSection(
            title="Resource",
            content=[
                f"Type: {resource_name}",
                f"Identifier: {identifier!s}",
                f"Source file: {source_file.as_posix()}",
            ],
            content_padding=_SECTION_BODY_PADDING,
        ),
        ToolkitPanelSection(
            title="Summary",
            content=summary_lines,
            content_padding=_SECTION_BODY_PADDING,
        ),
        ToolkitPanelSection(
            content=[_build_side_by_side_table(cdf_lines, yaml_lines, cdf_project=cdf_project)],
        ),
    ]

    return ToolkitPanel(
        Group(*sections),
        title=f"[bold]Diff view[/] - {resource_name}: {identifier!s}",
        border_style=AuraColor.AMBER.rich,
    )
