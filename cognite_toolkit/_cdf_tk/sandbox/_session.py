"""In-memory session over the resources of one or more modules, built on the build's own loading and rules."""

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, field
from difflib import unified_diff
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildVariable,
    BuiltModule,
    InsightList,
    Module,
    ModuleDirectory,
    SuccessfulReadYAMLFile,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    BuildWarning,
    ConsistencyError,
    FileReadError,
    Insight,
    InsightDefinition,
    ModelSyntaxError,
    ModelSyntaxWarning,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    SUPPORTS_VARIABLE_REPLACEMENT,
    ModuleId,
    ReadResource,
)
from cognite_toolkit._cdf_tk.resource_ios import RESOURCE_BUILD_IO_BY_TYPE, RESOURCE_BUILD_IO_LIST
from cognite_toolkit._cdf_tk.resource_ios._base_ios import BaseResourceIO, ResourceFileIO, SuccessExtra
from cognite_toolkit._cdf_tk.rules import LocalRulesOrchestrator
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException
from cognite_toolkit._cdf_tk.sandbox._render import render_source_file
from cognite_toolkit._cdf_tk.sandbox._source import SourceFile, TextFile
from cognite_toolkit._cdf_tk.utils.file import safe_read, safe_write, sanitize_filename
from cognite_toolkit._cdf_tk.validation import humanize_validation_error_categorized

BLOCKING_INSIGHT_TYPES = (ModelSyntaxError, ConsistencyError, BuildError, FileReadError)

SessionInsight = Insight | InternalValidatorException


@dataclass
class ApplyResult:
    ok: bool
    insights: list[SessionInsight] = field(default_factory=list)


@dataclass
class ResourceRef:
    source: SourceFile
    index: int

    @property
    def resource(self) -> ReadResource:
        return self.source.read.resources[self.index]


@dataclass
class _Journal:
    """What an operation changed, so that it can be rolled back."""

    sources: dict[int, tuple[SourceFile, list[dict[str, Any]], list[dict[str, Any]], bool]] = field(
        default_factory=dict
    )
    added: list[SourceFile] = field(default_factory=list)
    texts: dict[Path, tuple[TextFile, str, list[tuple[SuccessExtra, str | None]]]] = field(default_factory=dict)


def is_blocking_insight(insight: SessionInsight) -> bool:
    return isinstance(insight, BLOCKING_INSIGHT_TYPES)


def _fingerprint(insight: InsightDefinition) -> tuple[str, str, str, str]:
    return type(insight).__name__, insight.code, insight.message, str(insight.source_file)


def _child(container: Any, step: str | int) -> Any:
    if isinstance(container, dict) and isinstance(step, str) and step in container:
        return container[step]
    if isinstance(container, list) and isinstance(step, int) and -len(container) <= step < len(container):
        return container[step]
    raise ValueError(f"Cannot step into {step!r}: it does not exist")


class ModelingSession:
    """Resources of the loaded modules, which can be edited and written back.

    Files are loaded with the same code as ``cdf build``, edited through generic path operations, and checked
    with the build's local and global rules after each operation.
    """

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.modules: dict[ModuleId, Module] = {}
        self.files: list[SourceFile] = []
        self._variables: dict[ModuleId, list[BuildVariable]] = {}
        self._texts: dict[Path, TextFile] = {}
        self._built: dict[ModuleId, BuiltModule] = {}
        self._insights: list[SessionInsight] = []
        self._journal = _Journal()
        self._command = BuildV2Command(print_warning=False, skip_tracking=True, silent=True)
        self._local_rules = LocalRulesOrchestrator()

    def add_module(self, source: ModuleDirectory) -> None:
        module = self._command.import_module(source)
        self.modules[module.id] = module
        self._variables[module.id] = source.variables
        for read in module.files:
            if isinstance(read, SuccessfulReadYAMLFile):
                crud_cls = RESOURCE_BUILD_IO_BY_TYPE[read.source_type]
                self.files.append(SourceFile.load(read, module.id, crud_cls, source.variables))
        self._insights = self._check()

    def resources(self) -> list[ResourceRef]:
        return [ResourceRef(source, index) for source in self.files for index in range(len(source.resolved))]

    def find(self, kind: str, identifier: dict[str, Any], folder: str | None = None) -> ResourceRef:
        matches = [
            ref
            for ref in self.resources()
            if ref.source.read.source_type.kind.lower() == kind.lower()
            and (folder is None or ref.source.read.source_type.resource_folder == folder)
            and ref.resource.identifier.dump() == identifier
        ]
        if not matches:
            raise KeyError(f"No {kind} with identifier {identifier} is loaded in the session")
        if len(matches) > 1:
            folders = sorted({ref.source.read.source_type.resource_folder for ref in matches})
            raise ValueError(f"{kind} {identifier} exists in several folders, pass one of {folders}")
        return matches[0]

    def extra_texts(self, ref: ResourceRef) -> dict[str, TextFile]:
        """The text files of a resource by the resource field they fill, for example the SQL of a transformation."""
        return {
            extra.resource_field: self._text_file(extra.source_path)
            for extra in ref.resource.extra_files
            if isinstance(extra, SuccessExtra) and extra.resource_field and extra.content is not None
        }

    def _text_file(self, path: Path) -> TextFile:
        if path not in self._texts:
            text = safe_read(path)
            self._texts[path] = TextFile(path=path, original_text=text, text=text)
        return self._texts[path]

    def _touch(self, source: SourceFile) -> None:
        self._journal.sources.setdefault(
            id(source), (source, deepcopy(source.resolved), deepcopy(source.placeholder), source.dirty)
        )
        source.dirty = True

    def set_path(self, ref: ResourceRef, path: list[str | int], value: Any) -> None:
        """Set the value at the path. ``"-"`` as the last step appends to a list."""
        resolved_parent, placeholder_parent = self._parents(ref, path)
        self._touch(ref.source)
        for parent in (resolved_parent, placeholder_parent):
            step, new_value = path[-1], deepcopy(value)
            if isinstance(parent, list) and step == "-":
                parent.append(new_value)
            elif isinstance(parent, list) and isinstance(step, int):
                parent[step] = new_value
            elif isinstance(parent, dict) and isinstance(step, str):
                parent[step] = new_value
            else:
                raise ValueError(f"Cannot set {step!r} on a {type(parent).__name__}")

    def remove_path(self, ref: ResourceRef, path: list[str | int]) -> None:
        resolved_parent, placeholder_parent = self._parents(ref, path)
        _child(resolved_parent, path[-1])
        self._touch(ref.source)
        del resolved_parent[path[-1]]
        del placeholder_parent[path[-1]]

    def _parents(self, ref: ResourceRef, path: list[str | int]) -> tuple[Any, Any]:
        """The containers holding the last step of the path, in the resolved and the placeholder form."""
        resolved, placeholder = ref.source.resolved[ref.index], ref.source.placeholder[ref.index]
        for step in path[:-1]:
            resolved, placeholder = _child(resolved, step), _child(placeholder, step)
            if type(resolved) is not type(placeholder) or (
                isinstance(resolved, list) and len(resolved) != len(placeholder)
            ):
                raise ValueError(f"Cannot edit below {step!r}: its shape comes from a variable")
        return resolved, placeholder

    def set_extra(self, ref: ResourceRef, resource_field: str, text: str) -> None:
        """Replace the text file filling a resource field. Keep ``{{ variable }}`` tokens in the text."""
        extras = [
            extra
            for extra in ref.resource.extra_files
            if isinstance(extra, SuccessExtra) and extra.resource_field == resource_field
        ]
        if not extras or extras[0].content is None:
            raise ValueError(f"The resource has no text file for field {resource_field!r}")
        text_file = self._text_file(extras[0].source_path)
        self._journal.texts.setdefault(
            text_file.path, (text_file, text_file.text, [(extra, extra.content) for extra in extras])
        )
        text_file.text = text
        for extra in extras:
            extra.content = (
                BuildVariable.substitute(text, ref.source.variables, extra.suffix)
                if extra.suffix in SUPPORTS_VARIABLE_REPLACEMENT
                else text
            )

    def add_resource(
        self,
        kind: str,
        resource: dict[str, Any],
        folder: str | None = None,
        module: str | None = None,
        sub_dir: str | None = None,
    ) -> Identifier:
        """Add a resource in a new file. The file is placed below the module, in the resource folder of the kind."""
        crud_cls = self.crud_class(kind, folder)
        target = self._module(module)
        identifier = crud_cls.validate_object(deepcopy(resource), extra="forbid").as_id()
        if any(ref.resource.identifier == identifier and ref.source.crud_cls is crud_cls for ref in self.resources()):
            raise ValueError(f"{kind} {identifier} already exists in the session")

        directory = target.id.path / crud_cls.folder_name
        if sub_dir is None:
            sub_dir = crud_cls.sub_folder_name or ""
        name = sanitize_filename(getattr(identifier, "external_id", str(identifier)))
        path = (directory / sub_dir / f"{name}.{crud_cls.kind}.yaml").resolve()
        if path.exists() or any(source.path == path for source in self.files):
            raise ValueError(f"{path} already exists")

        source = SourceFile.create_new(path, target.id, crud_cls, resource, identifier, self._variables[target.id])
        target.files.append(source.read)
        self.files.append(source)
        self._journal.added.append(source)
        self._refresh(source)
        return identifier

    def crud_class(self, kind: str, folder: str | None) -> type[BaseResourceIO]:
        matches = [
            io_cls
            for io_cls in RESOURCE_BUILD_IO_LIST
            if io_cls.kind.lower() == kind.lower() and (folder is None or io_cls.folder_name == folder)
        ]
        if len(matches) != 1:
            options = sorted(io_cls.folder_name for io_cls in matches)
            raise ValueError(
                f"Unknown kind {kind!r}" if not matches else f"{kind!r} exists in {options}, pass a folder"
            )
        return matches[0]

    def _module(self, module: str | None) -> Module:
        if module is None and len(self.modules) == 1:
            return next(iter(self.modules.values()))
        for candidate in self.modules.values():
            if candidate.id.id.as_posix() == module:
                return candidate
        raise ValueError(
            f"Pass the module to add the resource to, one of {[m.id.id.as_posix() for m in self.modules.values()]}"
        )

    def _refresh(self, source: SourceFile) -> None:
        """Validate the edited file again, and update the resources the rules read."""
        read = source.read
        messages: list[str] = []
        warnings: list[ModelSyntaxWarning | BuildWarning] = []
        for resolved, resource in zip(source.resolved, read.resources):
            try:
                validated = source.crud_cls.validate_object(deepcopy(resolved), extra="forbid")
            except ValidationError as error:
                resource.validated = None
                found = humanize_validation_error_categorized(error, source.crud_cls.yaml_cls, for_insights=True)
                messages.extend(item.message for item in found if item.category == "error")
                warnings.extend(
                    BuildWarning(code=item.code, message=item.message, source_file=source.path)
                    for item in found
                    if item.category == "warning"
                )
                if not found:
                    messages.append(str(error))
                continue
            resource.validated = validated
            resource.identifier = validated.as_id()
            resource.raw = (
                source.crud_cls.to_crud_type(resolved)
                if issubclass(source.crud_cls, ResourceFileIO)
                else deepcopy(resolved)
            )
            warnings.extend(validated.syntax_warnings(source.path))
        read.syntax_error = (
            BuildError(
                code="INVALID-FIELD",
                message="\n".join(messages),
                fix="Compare the YAML with reference documentation and make sure it is valid.",
                source_file=source.path,
            )
            if messages
            else None
        )
        read.syntax_warnings = warnings
        self._built.pop(source.module_id, None)

    def apply(self, operation: Any) -> ApplyResult:
        """Apply an operation. It is rolled back if it introduces blocking insights."""
        self._journal = _Journal()
        try:
            operation.apply(self)
            for source, *_ in self._journal.sources.values():
                self._refresh(source)
        except Exception:
            self._rollback()
            raise

        insights = self._check()
        known = {_fingerprint(insight) for insight in self._insights if isinstance(insight, InsightDefinition)}
        if any(
            isinstance(insight, InsightDefinition)
            and is_blocking_insight(insight)
            and _fingerprint(insight) not in known
            for insight in insights
        ):
            self._rollback()
            return ApplyResult(ok=False, insights=insights)
        self._insights = insights
        return ApplyResult(ok=True, insights=insights)

    def _rollback(self) -> None:
        journal = self._journal
        for source in journal.added:
            self.files.remove(source)
            for module in self.modules.values():
                if source.read in module.files:
                    module.files.remove(source.read)
            self._built.pop(source.module_id, None)
        for source, resolved, placeholder, dirty in journal.sources.values():
            source.resolved, source.placeholder, source.dirty = resolved, placeholder, dirty
            self._refresh(source)
        for text_file, text, extras in journal.texts.values():
            text_file.text = text
            for extra, content in extras:
                extra.content = content

    def validate(self) -> list[SessionInsight]:
        self._built.clear()
        self._insights = self._check()
        return self._insights

    def _check(self) -> list[SessionInsight]:
        """All insights of the loaded modules: the build's syntax findings plus the local and global rules."""
        for module in self.modules.values():
            if module.id not in self._built:
                self._built[module.id] = self._command.build_module(
                    module, self._variables[module.id], Counter(), None, self._local_rules
                )
        built_modules = list(self._built.values())
        results = self._command.run_global_rules(built_modules)

        found = InsightList()
        ignored_by_source: dict[Path, set[str]] = {}
        for built in built_modules:
            found.extend(built.all_insights)
            for path, rules in built.ignore_rules_by_source.items():
                ignored_by_source.setdefault(path, set()).update(rules)
        for result in results:
            found.extend(result.insights)
        reported = self._command.filter_to_reported_insights(found, ignored_by_source, set())
        return [*reported, *(error for result in results for error in result.errors)]

    def diff(self) -> dict[Path, str]:
        """Unified diffs for the files that are edited."""
        diffs: dict[Path, str] = {}
        for source in self.files:
            if source.dirty or source.is_new:
                self._add_diff(diffs, source.path, source.original_text, render_source_file(source))
        for text_file in self._texts.values():
            self._add_diff(diffs, text_file.path, text_file.original_text, text_file.text)
        return diffs

    @staticmethod
    def _add_diff(diffs: dict[Path, str], path: Path, original: str, updated: str) -> None:
        if original != updated:
            lines = unified_diff(
                original.splitlines(keepends=True),
                updated.splitlines(keepends=True),
                fromfile=str(path),
                tofile=str(path),
            )
            diffs[path] = "".join(lines)

    def write(self) -> list[Path]:
        """Write the edited files to disk. Returns the paths written."""
        written: list[Path] = []
        for source in self.files:
            if not source.dirty and not source.is_new:
                continue
            content = render_source_file(source)
            source.path.parent.mkdir(parents=True, exist_ok=True)
            safe_write(source.path, content)
            source.original_text = content
            source.baseline = deepcopy(source.placeholder)
            source.dirty = source.is_new = False
            written.append(source.path)
        for text_file in self._texts.values():
            if text_file.dirty:
                safe_write(text_file.path, text_file.text)
                text_file.original_text = text_file.text
                written.append(text_file.path)
        return written
