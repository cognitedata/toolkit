"""Render edited source files back to YAML with comments and variables preserved."""

from typing import Any

from cognite_toolkit._cdf_tk.data_classes import YAMLComments
from cognite_toolkit._cdf_tk.sandbox._source import SourceFile
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


def new_properties_only(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any] | None:
    """If the only change is new entries under ``properties``, return those entries."""
    if "properties" not in baseline or set(current) - set(baseline) - {"properties"}:
        return None
    for key, value in baseline.items():
        if key != "properties" and current.get(key) != value:
            return None

    baseline_props = baseline["properties"] or {}
    current_props = current.get("properties") or {}
    if not isinstance(baseline_props, dict) or not isinstance(current_props, dict):
        return None
    if any(current_props.get(key) != value for key, value in baseline_props.items()):
        return None
    return {key: value for key, value in current_props.items() if key not in baseline_props} or None


def _indent_block(block: str, spaces: int) -> str:
    prefix = " " * spaces
    return "".join(prefix + line if line.strip() else line for line in block.splitlines(keepends=True))


def splice_properties_into_source(original_text: str, new_properties: dict[str, Any]) -> str:
    """Append new property entries into an existing ``properties:`` section.

    Leaves every existing line of ``original_text`` untouched. The new block is
    dumped with ``yaml_safe_dump`` and indented to sit under ``properties``.
    """
    prop_yaml = yaml_safe_dump(new_properties)
    if not prop_yaml.endswith("\n"):
        prop_yaml += "\n"
    block = _indent_block(prop_yaml, 2)

    lines = original_text.splitlines(keepends=True)
    props_start: int | None = None
    for index, line in enumerate(lines):
        if line.startswith("properties:"):
            props_start = index
            break
    if props_start is None:
        raise ValueError("Cannot splice properties: no top-level 'properties:' section")

    end = len(lines)
    for index in range(props_start + 1, len(lines)):
        line = lines[index]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line[0].isspace():
            end = index
            break

    # Keep a trailing newline on the file when the original had one.
    body = lines[:end]
    if body and not body[-1].endswith("\n"):
        body[-1] = body[-1] + "\n"
    result = "".join(body) + block + "".join(lines[end:])
    if original_text.endswith("\n") and not result.endswith("\n"):
        result += "\n"
    return result


def _restore_original_tokens(dumped: str, token_by_placeholder: dict[str, str]) -> str:
    for placeholder, token in token_by_placeholder.items():
        dumped = dumped.replace(placeholder, token)
    return dumped


def render_source_file(source: SourceFile) -> str:
    """Render an edited source file to YAML text ready to write."""
    content: dict[str, Any] | list[dict[str, Any]] = source.placeholder if source.is_list else source.placeholder[0]
    if source.is_new:
        return yaml_safe_dump(content)

    # Prefer a surgical splice when the only change is new properties. A full
    # dump rewrites quoting and {{ }} spacing on real projects.
    if (
        source.baseline
        and not source.is_list
        and (new := new_properties_only(source.placeholder[0], source.baseline[0]))
    ):
        return _restore_original_tokens(
            splice_properties_into_source(source.original_text, new), source.token_by_placeholder
        )

    dumped = _restore_original_tokens(yaml_safe_dump(content), source.token_by_placeholder)
    rendered = YAMLComments.load(source.original_text).dump(dumped)
    # YAMLComments.dump joins with "\n" and drops a trailing newline; restore it.
    if source.original_text.endswith("\n") and not rendered.endswith("\n"):
        rendered += "\n"
    return rendered
