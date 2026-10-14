"""Source files tracked by the sandbox, in both resolved and placeholder form."""

import re
import uuid
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable, SuccessfulReadYAMLFile
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId, ReadResource
from cognite_toolkit._cdf_tk.resource_ios._base_ios import BaseResourceIO
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, safe_read


def _as_resource_dicts(content: dict[str, Any] | list[dict[str, Any]], path: Path) -> list[dict[str, Any]]:
    if isinstance(content, dict):
        return [content]
    if isinstance(content, list) and all(isinstance(item, dict) for item in content):
        return content
    raise ValueError(f"Expected a mapping or list of mappings in {path}, got {type(content)}")


def substitute_with_original_tokens(
    content: str, variables: list[BuildVariable]
) -> tuple[str, dict[str, str], dict[str, BuildVariable]]:
    """Replace ``{{ variable }}`` tokens with placeholders, keeping the original spelling.

    Unlike ``BuildVariable.substitute_with_placeholders``, the restore map preserves
    the exact token text from the file (spacing and surrounding characters are not
    normalized to ``{{ name }}``). Also returns ``placeholder -> BuildVariable`` for
    CDF merge (``ResourceReplacer``).
    """
    token_by_placeholder: dict[str, str] = {}
    variable_by_placeholder: dict[str, BuildVariable] = {}
    for variable in variables:
        pattern = re.compile(rf"{{{{\s*{re.escape(variable.name)}\s*}}}}")

        def _replace(match: re.Match[str]) -> str:
            placeholder = f"VARIABLE_{uuid.uuid4().hex[:8]}"
            token_by_placeholder[placeholder] = match.group(0)
            variable_by_placeholder[placeholder] = variable
            return placeholder

        content = pattern.sub(_replace, content)
    return content, token_by_placeholder, variable_by_placeholder


@dataclass
class SourceFile:
    """One resource YAML file on disk.

    ``read`` is the file as the build sees it. ``resolved`` and ``placeholder`` hold the same
    content with ``{{ variable }}`` tokens substituted and replaced by placeholders respectively, one
    entry per resource in the file. Edits are applied to both, and the placeholder form is what is written back.
    """

    read: SuccessfulReadYAMLFile
    module_id: ModuleId
    crud_cls: type[BaseResourceIO]
    variables: list[BuildVariable]
    original_text: str
    token_by_placeholder: dict[str, str]
    variable_by_placeholder: dict[str, BuildVariable]
    resolved: list[dict[str, Any]]
    placeholder: list[dict[str, Any]]
    is_list: bool
    dirty: bool = False
    is_new: bool = False
    # Marked for deletion on the next write (RemoveResource).
    deleted: bool = False
    # The placeholder form at load or last write, to recognise edits that only add properties.
    baseline: list[dict[str, Any]] | None = None

    @property
    def path(self) -> Path:
        return self.read.source_path

    @classmethod
    def load(
        cls,
        read: SuccessfulReadYAMLFile,
        module_id: ModuleId,
        crud_cls: type[BaseResourceIO],
        variables: list[BuildVariable],
    ) -> Self:
        original_text = safe_read(read.source_path)
        text = crud_cls.safe_read(original_text)
        placeholder_text, token_by_placeholder, variable_by_placeholder = substitute_with_original_tokens(
            text, variables
        )
        resolved_content = read_yaml_content(crud_cls.substitute_variables_content(text, variables))
        placeholder_content = read_yaml_content(placeholder_text)

        if type(resolved_content) is not type(placeholder_content):
            raise ValueError(f"Resolved and placeholder YAML structures differ in {read.source_path}")
        resolved = _as_resource_dicts(resolved_content, read.source_path)
        placeholder = _as_resource_dicts(placeholder_content, read.source_path)
        if len(resolved) != len(placeholder):
            raise ValueError(f"Resolved and placeholder resource counts differ in {read.source_path}")

        return cls(
            read=read,
            module_id=module_id,
            crud_cls=crud_cls,
            variables=variables,
            original_text=original_text,
            token_by_placeholder=token_by_placeholder,
            variable_by_placeholder=variable_by_placeholder,
            resolved=resolved,
            placeholder=placeholder,
            is_list=isinstance(resolved_content, list),
            baseline=deepcopy(placeholder),
        )

    @classmethod
    def create_new(
        cls,
        path: Path,
        module_id: ModuleId,
        crud_cls: type[BaseResourceIO],
        resource: dict[str, Any],
        identifier: Identifier,
        variables: list[BuildVariable],
    ) -> Self:
        """A file for a resource that does not exist on disk yet."""
        read = SuccessfulReadYAMLFile(
            source_path=path,
            source_hash="sandbox",
            resource_type=crud_cls.as_crud_type(),
            source_type=crud_cls.as_resource_type(),
            resources=[ReadResource(raw=deepcopy(resource), identifier=identifier)],
            line_count=0,
        )
        return cls(
            read=read,
            module_id=module_id,
            crud_cls=crud_cls,
            variables=variables,
            original_text="",
            token_by_placeholder={},
            variable_by_placeholder={},
            resolved=[deepcopy(resource)],
            placeholder=[deepcopy(resource)],
            is_list=False,
            dirty=True,
            is_new=True,
        )


@dataclass
class TextFile:
    """A text file next to a resource, such as the SQL query of a transformation.

    ``text`` keeps the ``{{ variable }}`` tokens, so that write-back preserves them.
    """

    path: Path
    original_text: str
    text: str

    @property
    def dirty(self) -> bool:
        return self.text != self.original_text
