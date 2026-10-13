"""In-memory modeling session backed by *Request pydantic classes."""

from copy import deepcopy
from dataclasses import dataclass, field
from difflib import unified_diff
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Self, cast

from cognite_toolkit._cdf_tk.client._resource_base import Identifier, RequestResource
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ExternalId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    DataModelRequest,
    SpaceRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, DataModelIO, ResourceIO, SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException
from cognite_toolkit._cdf_tk.sandbox._render import render_source_file, write_source_file
from cognite_toolkit._cdf_tk.sandbox._source import SourceFile, SqlSource
from cognite_toolkit._cdf_tk.sandbox._validation import (
    ResourceLocation,
    is_blocking_insight,
    validate_session,
)
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename

SUPPORTED_KINDS: dict[str, type[ResourceIO]] = {
    "space": SpaceIO,
    "container": ContainerIO,
    "view": ViewIO,
    "datamodel": DataModelIO,
}


@dataclass
class ApplyResult:
    ok: bool
    insights: list[Insight | InternalValidatorException] = field(default_factory=list)


@dataclass
class _IndexEntry:
    source: SourceFile
    index: int


class ModelingSession:
    """Loaded data modeling resources that can be mutated and written back."""

    def __init__(
        self,
        root: Path,
        variables: list[BuildVariable] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.variables = variables or []
        self.files: list[SourceFile] = []
        self.requests: dict[Identifier, RequestResource] = {}
        self.queries: dict[ExternalId, SqlSource] = {}
        self._index: dict[Identifier, _IndexEntry] = {}
        self._locations: dict[Identifier, ResourceLocation] = {}
        self._ios: dict[type[ResourceIO], ResourceIO] = {}

    def _io(self, crud_cls: type[ResourceIO]) -> ResourceIO:
        if crud_cls not in self._ios:
            client = cast(Any, SimpleNamespace(console=None))
            self._ios[crud_cls] = crud_cls(client)
        return self._ios[crud_cls]

    @classmethod
    def load(cls, root: Path, variables: list[BuildVariable] | None = None) -> Self:
        session = cls(root=root, variables=variables)
        session.load_dir(root, session.variables)
        return session

    def load_dir(self, directory: Path, variables: list[BuildVariable]) -> None:
        """Load all resources below a directory, resolving them with the given variables."""
        for path in sorted(directory.resolve().rglob("*.yaml")):
            if path.stem.lower().endswith(".transformation"):
                self._load_transformation(path, variables)
                continue
            kind = self._kind_from_path(path)
            if kind is None:
                continue
            crud_cls = SUPPORTED_KINDS[kind]
            crud = self._io(crud_cls)
            source = SourceFile.load(path, crud, variables)
            self.files.append(source)
            for index, raw in enumerate(source.resolved):
                request = crud.load_resource(deepcopy(raw))
                identifier = crud.get_id(request)
                if identifier in self.requests:
                    raise ValueError(
                        f"Duplicate identifier {identifier} in {path} and {self._locations[identifier].path}"
                    )
                self.requests[identifier] = request
                self._index[identifier] = _IndexEntry(source=source, index=index)
                self._locations[identifier] = ResourceLocation(path=source.path, crud=crud, variables=variables)
                source.baseline_dumps.append(request.dump())

    def _load_transformation(self, path: Path, variables: list[BuildVariable]) -> None:
        for sql in SqlSource.load_all(path, variables):
            if sql.transformation in self.queries:
                raise ValueError(f"Duplicate transformation {sql.transformation} in {path}")
            self.queries[sql.transformation] = sql

    def get_query(self, transformation: ExternalId) -> SqlSource:
        try:
            return self.queries[transformation]
        except KeyError as error:
            raise KeyError(f"Transformation {transformation} has no SQL file in the session") from error

    def set_query(self, transformation: ExternalId, query: str) -> None:
        source = self.get_query(transformation)
        source.text = query
        source.is_dirty = query != source.original_text

    @staticmethod
    def _kind_from_path(path: Path) -> str | None:
        if "." not in path.stem:
            return None
        kind = path.stem.rsplit(".", maxsplit=1)[-1].lower()
        return kind if kind in SUPPORTED_KINDS else None

    def get(self, identifier: Identifier) -> RequestResource:
        try:
            return self.requests[identifier]
        except KeyError as error:
            raise KeyError(f"Resource {identifier} is not loaded in the session") from error

    def get_container(self, container_id: ContainerId) -> ContainerRequest:
        request = self.get(container_id)
        if not isinstance(request, ContainerRequest):
            raise TypeError(f"Expected ContainerRequest for {container_id}, got {type(request).__name__}")
        return request

    def get_view(self, view_id: ViewId) -> ViewRequest:
        request = self.get(view_id)
        if not isinstance(request, ViewRequest):
            raise TypeError(f"Expected ViewRequest for {view_id}, got {type(request).__name__}")
        return request

    def get_data_model(self, data_model_id: DataModelId) -> DataModelRequest:
        request = self.get(data_model_id)
        if not isinstance(request, DataModelRequest):
            raise TypeError(f"Expected DataModelRequest for {data_model_id}, got {type(request).__name__}")
        return request

    def get_space(self, space_id: SpaceId) -> SpaceRequest:
        request = self.get(space_id)
        if not isinstance(request, SpaceRequest):
            raise TypeError(f"Expected SpaceRequest for {space_id}, got {type(request).__name__}")
        return request

    def mark_dirty(self, identifier: Identifier) -> None:
        self._index[identifier].source.dirty = True

    def add_request(
        self,
        request: RequestResource,
        crud_cls: type[ResourceIO],
        sub_folder: str | None = None,
        relative_dir: Path | None = None,
    ) -> Identifier:
        """Register a new request and create a dirty SourceFile for it.

        Args:
            relative_dir: Optional path relative to the session root where the
                file should be written (for example ``dm_ent_emergency360/containers``).
                When set, ``sub_folder`` is ignored.
        """
        crud = self._io(crud_cls)
        identifier = crud.get_id(request)
        if identifier in self.requests:
            raise ValueError(f"Resource {identifier} already exists in the session")

        filename = f"{sanitize_filename(getattr(identifier, 'external_id', str(identifier)))}.{crud.kind}.yaml"
        if relative_dir is not None:
            folder = self.root / relative_dir
        else:
            folder = self.root
            if sub_folder:
                folder = folder / sub_folder
        path = folder / filename

        dumped = request.dump()
        source = SourceFile.create_new(path, crud, dumped, self.variables)
        self.files.append(source)
        self.requests[identifier] = request
        self._index[identifier] = _IndexEntry(source=source, index=0)
        self._locations[identifier] = ResourceLocation(path=path, crud=crud, variables=self.variables)
        return identifier

    def apply(self, operation: Any) -> ApplyResult:
        """Apply an operation with validation and rollback on blocking insights."""
        snapshot = {identifier: request.model_copy(deep=True) for identifier, request in self.requests.items()}
        dirty_before = {id(source): source.dirty for source in self.files}
        files_before = list(self.files)
        index_before = dict(self._index)
        locations_before = dict(self._locations)
        queries_before = {identifier: source.text for identifier, source in self.queries.items()}

        try:
            operation.apply(self)
        except Exception:
            self._restore(snapshot, dirty_before, files_before, index_before, locations_before, queries_before)
            raise

        dirty_ids = {
            identifier for identifier, entry in self._index.items() if entry.source.dirty or entry.source.is_new
        }
        insights = validate_session(self.requests, self._locations, self.queries, syntax_ids=dirty_ids)
        if any(is_blocking_insight(insight) for insight in insights):
            self._restore(snapshot, dirty_before, files_before, index_before, locations_before, queries_before)
            return ApplyResult(ok=False, insights=insights)
        return ApplyResult(ok=True, insights=insights)

    def _restore(
        self,
        snapshot: dict[Identifier, RequestResource],
        dirty_before: dict[int, bool],
        files_before: list[SourceFile],
        index_before: dict[Identifier, _IndexEntry],
        locations_before: dict[Identifier, ResourceLocation],
        queries_before: dict[ExternalId, str],
    ) -> None:
        self.requests = snapshot
        self.files = files_before
        self._index = index_before
        self._locations = locations_before
        for source in self.files:
            source.dirty = dirty_before.get(id(source), source.dirty)
        for identifier, text in queries_before.items():
            self.set_query(identifier, text)

    def validate(self) -> list[Insight | InternalValidatorException]:
        return validate_session(self.requests, self._locations, self.queries, syntax_ids=None)

    def _requests_by_index(self, source: SourceFile) -> dict[int, RequestResource]:
        by_index: dict[int, RequestResource] = {}
        for identifier, entry in self._index.items():
            if entry.source is source:
                by_index[entry.index] = self.requests[identifier]
        return by_index

    def diff(self, include_clean: bool = False) -> dict[Path, str]:
        """Return unified diffs for dirty files (or all files if include_clean)."""
        diffs: dict[Path, str] = {}
        for source in self.files:
            if not include_clean and not source.dirty and not source.is_new:
                continue
            rendered = render_source_file(source, self._requests_by_index(source))
            original = source.original_text
            if rendered == original:
                continue
            diff_lines = unified_diff(
                original.splitlines(keepends=True),
                rendered.splitlines(keepends=True),
                fromfile=str(source.path),
                tofile=str(source.path),
            )
            diffs[source.path] = "".join(diff_lines)
        for sql in self.queries.values():
            if sql.text != sql.original_text:
                diffs[sql.path] = "".join(
                    unified_diff(
                        sql.original_text.splitlines(keepends=True),
                        sql.text.splitlines(keepends=True),
                        fromfile=str(sql.path),
                        tofile=str(sql.path),
                    )
                )
        return diffs

    def write(self) -> list[Path]:
        """Write dirty files to disk. Returns the paths written."""
        written: list[Path] = []
        for source in self.files:
            if not source.dirty and not source.is_new:
                continue
            written.append(write_source_file(source, self._requests_by_index(source)))
        for sql in self.queries.values():
            if sql.is_dirty:
                written.append(sql.write())
        return written

    def force_render_all(self) -> dict[Path, str]:
        """Render every file as if dirty. Used by the no-op round-trip measurement."""
        for source in self.files:
            source.dirty = True
        return self.diff(include_clean=True)
