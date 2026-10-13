"""Render dirty source files back to YAML with comments and variables preserved."""

from copy import deepcopy
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.client._resource_base import RequestResource
from cognite_toolkit._cdf_tk.commands.pull import ResourceReplacer
from cognite_toolkit._cdf_tk.data_classes import YAMLComments
from cognite_toolkit._cdf_tk.sandbox._source import SourceFile
from cognite_toolkit._cdf_tk.utils.file import safe_write, yaml_safe_dump


def align_dump_to_local(
    dumped: Any,
    local: Any,
    baseline: Any,
) -> Any:
    """Drop keys that the request dump adds but the original file never had.

    New keys (present in dumped, absent from local) are kept as-is so additive
    operations survive. Keys that exist on both sides are recursed into; keys
    that appear in both dumped and the load-time baseline but not in local are
    stripped as serializer noise (for example ``type: container`` on a view
    property reference).
    """
    if isinstance(dumped, dict) and isinstance(local, dict):
        baseline_dict = baseline if isinstance(baseline, dict) else {}
        result: dict[str, Any] = {}
        for key, value in dumped.items():
            if key not in local:
                # Keys the dump always had but the author never wrote are noise.
                # Keys absent from both local and the load-time baseline are new.
                if key in baseline_dict:
                    continue
                result[key] = deepcopy(value)
                continue
            result[key] = align_dump_to_local(value, local[key], baseline_dict.get(key))
        return result
    if isinstance(dumped, list) and isinstance(local, list):
        baseline_list = baseline if isinstance(baseline, list) else []
        result_list: list[Any] = []
        for index, value in enumerate(dumped):
            if index < len(local):
                baseline_item = baseline_list[index] if index < len(baseline_list) else None
                result_list.append(align_dump_to_local(value, local[index], baseline_item))
            else:
                result_list.append(deepcopy(value))
        return result_list
    return deepcopy(dumped)


def new_properties_only(
    aligned: dict[str, Any],
    resolved: dict[str, Any],
) -> dict[str, Any] | None:
    """If the only change is new entries under ``properties``, return those entries."""
    if set(aligned) - set(resolved) - {"properties"}:
        return None
    for key, value in resolved.items():
        if key == "properties":
            continue
        if key not in aligned or aligned[key] != value:
            return None

    resolved_props = resolved.get("properties") or {}
    aligned_props = aligned.get("properties") or {}
    if not isinstance(resolved_props, dict) or not isinstance(aligned_props, dict):
        return None
    if any(key not in aligned_props for key in resolved_props):
        return None
    for key, value in resolved_props.items():
        if aligned_props.get(key) != value:
            return None

    new_props = {key: value for key, value in aligned_props.items() if key not in resolved_props}
    return new_props or None


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


def render_source_file(
    source: SourceFile,
    requests_by_index: dict[int, RequestResource],
) -> str:
    """Render a dirty source file to YAML text ready to write."""
    if source.is_new:
        items = [requests_by_index[index].dump() for index in sorted(requests_by_index)]
        content: dict[str, Any] | list[dict[str, Any]] = items if source.is_list else items[0]
        return yaml_safe_dump(content)

    # Prefer a surgical splice when the only change is new properties. Full
    # yaml_safe_dump rewrites quoting and {{ }} spacing on real projects.
    if not source.is_list and len(source.resolved) == 1 and 0 in requests_by_index:
        baseline = source.baseline_dumps[0] if source.baseline_dumps else {}
        aligned = align_dump_to_local(requests_by_index[0].dump(), source.resolved[0], baseline)
        if (new_props := new_properties_only(aligned, source.resolved[0])) is not None:
            return splice_properties_into_source(source.original_text, new_props)

    updated_items: list[dict[str, Any]] = []
    replacer = ResourceReplacer(source.value_by_placeholder, source.crud)
    for index, resolved in enumerate(source.resolved):
        request = requests_by_index.get(index)
        if request is None:
            updated_items.append(deepcopy(resolved))
            continue
        baseline = source.baseline_dumps[index] if index < len(source.baseline_dumps) else {}
        to_write = align_dump_to_local(request.dump(), resolved, baseline)
        updated_items.append(
            replacer.replace(
                resolved,
                source.placeholder[index],
                to_write,
            )
        )

    updated: dict[str, Any] | list[dict[str, Any]] = updated_items if source.is_list else updated_items[0]
    dumped = yaml_safe_dump(updated)
    dumped = _restore_original_tokens(dumped, source.token_by_placeholder)
    if source.original_text:
        # YAMLComments.dump joins with "\n" and drops a trailing newline; restore it.
        content = YAMLComments.load(source.original_text).dump(dumped)
        if source.original_text.endswith("\n") and not content.endswith("\n"):
            content += "\n"
        return content
    return dumped


def write_source_file(
    source: SourceFile,
    requests_by_index: dict[int, RequestResource],
) -> Path:
    """Render and write a dirty source file. Returns the path written."""
    content = render_source_file(source, requests_by_index)
    source.path.parent.mkdir(parents=True, exist_ok=True)
    safe_write(source.path, content)
    source.original_text = content
    source.dirty = False
    source.is_new = False
    return source.path
