"""Source file tracking for the modeling sandbox."""

import re
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO, TransformationIO
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, safe_read, safe_write


def _as_resource_dicts(content: dict[str, Any] | list[dict[str, Any]], path: Path) -> list[dict[str, Any]]:
    if isinstance(content, dict):
        return [content]
    if isinstance(content, list) and all(isinstance(item, dict) for item in content):
        return content
    raise ValueError(f"Expected a mapping or list of mappings in {path}, got {type(content)}")


def substitute_with_original_tokens(
    content: str, variables: list[BuildVariable]
) -> tuple[str, dict[str, BuildVariable], dict[str, str]]:
    """Replace ``{{ variable }}`` tokens with placeholders, keeping the original spelling.

    Unlike ``BuildVariable.substitute_with_placeholders``, the restore map preserves
    the exact token text from the file (spacing and surrounding characters are not
    normalized to ``{{ name }}``).
    """
    variable_by_placeholder: dict[str, BuildVariable] = {}
    token_by_placeholder: dict[str, str] = {}
    for variable in variables:
        pattern = re.compile(rf"{{{{\s*{re.escape(variable.name)}\s*}}}}")

        def _replace(match: re.Match[str], _variable: BuildVariable = variable) -> str:
            placeholder = f"VARIABLE_{uuid.uuid4().hex[:8]}"
            variable_by_placeholder[placeholder] = _variable
            token_by_placeholder[placeholder] = match.group(0)
            return placeholder

        content = pattern.sub(_replace, content)
    return content, variable_by_placeholder, token_by_placeholder


@dataclass
class SourceFile:
    """One YAML file on disk, with resolved and placeholder forms."""

    path: Path
    original_text: str
    crud: ResourceIO
    variables: list[BuildVariable]
    value_by_placeholder: dict[str, BuildVariable]
    token_by_placeholder: dict[str, str]
    resolved: list[dict[str, Any]]
    placeholder: list[dict[str, Any]]
    is_list: bool
    dirty: bool = False
    is_new: bool = False
    # Per-index baseline dumps from the request classes at load time.
    # Used to strip serializer noise that was not in the original file.
    baseline_dumps: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def load(
        cls,
        path: Path,
        crud: ResourceIO,
        variables: list[BuildVariable] | None = None,
    ) -> Self:
        variables = variables or []
        original_text = safe_read(path)
        placeholder_text, value_by_placeholder, token_by_placeholder = substitute_with_original_tokens(
            original_text, variables
        )
        resolved_text = BuildVariable.substitute(original_text, variables, path.suffix)

        resolved_content = read_yaml_content(crud.safe_read(resolved_text))
        placeholder_content = read_yaml_content(crud.safe_read(placeholder_text))

        if type(resolved_content) is not type(placeholder_content):
            raise ValueError(f"Resolved and placeholder YAML structures differ in {path}")

        is_list = isinstance(resolved_content, list)
        resolved_list = _as_resource_dicts(resolved_content, path)
        placeholder_list = _as_resource_dicts(placeholder_content, path)

        if len(resolved_list) != len(placeholder_list):
            raise ValueError(f"Resolved and placeholder resource counts differ in {path}")

        return cls(
            path=path.resolve(),
            original_text=original_text,
            crud=crud,
            variables=variables,
            value_by_placeholder=value_by_placeholder,
            token_by_placeholder=token_by_placeholder,
            resolved=[deepcopy(item) for item in resolved_list],
            placeholder=[deepcopy(item) for item in placeholder_list],
            is_list=is_list,
        )

    @classmethod
    def create_new(
        cls,
        path: Path,
        crud: ResourceIO,
        resource: dict[str, Any],
        variables: list[BuildVariable] | None = None,
    ) -> Self:
        """Create a SourceFile for a resource that does not yet exist on disk."""
        return cls(
            path=path.resolve(),
            original_text="",
            crud=crud,
            variables=variables or [],
            value_by_placeholder={},
            token_by_placeholder={},
            resolved=[deepcopy(resource)],
            placeholder=[deepcopy(resource)],
            is_list=False,
            dirty=True,
            is_new=True,
            baseline_dumps=[{}],
        )


@dataclass
class SqlSource:
    """The .sql query file next to a transformation YAML.

    ``text`` is the raw file text, with ``{{ variable }}`` tokens kept so that
    write-back preserves them.
    """

    path: Path
    original_text: str
    text: str
    variables: list[BuildVariable]
    transformation: ExternalId
    destination_view: ViewId | None
    is_dirty: bool = False

    @property
    def resolved_text(self) -> str:
        return BuildVariable.substitute(self.text, self.variables, ".sql")

    @classmethod
    def load_all(cls, yaml_path: Path, variables: list[BuildVariable]) -> list[Self]:
        content = read_yaml_content(BuildVariable.substitute(safe_read(yaml_path), variables))
        sources: list[Self] = []
        for item in _as_resource_dicts(content, yaml_path):
            identifier = ExternalId(external_id=item["externalId"])
            if "queryFile" in item:
                sql_path: Path | None = yaml_path.parent / str(item["queryFile"])
            else:
                sql_path = TransformationIO._try_get_adjacent_sql_file_implicitly(yaml_path, identifier)
            if sql_path is None or not sql_path.exists():
                continue
            text = safe_read(sql_path)
            sources.append(
                cls(
                    path=sql_path.resolve(),
                    original_text=text,
                    text=text,
                    variables=variables,
                    transformation=identifier,
                    destination_view=_destination_view(item.get("destination")),
                )
            )
        return sources

    def write(self) -> Path:
        safe_write(self.path, self.text)
        self.original_text = self.text
        self.is_dirty = False
        return self.path


def _destination_view(destination: Any) -> ViewId | None:
    if not isinstance(destination, dict):
        return None
    data_model = destination.get("dataModel")
    if not isinstance(data_model, dict) or not (destination_type := data_model.get("destinationType")):
        return None
    return ViewId(space=data_model["space"], external_id=destination_type, version=data_model["version"])
