"""Resource stores: YAML on disk and CDF, behind one load / plan / commit interface."""

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any, Protocol

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    DeploymentStep,
    DeployOptions,
    DeployV2Command,
    ReadResource,
    ResourceToDeploy,
)
from cognite_toolkit._cdf_tk.commands.pull import ResourceReplacer
from cognite_toolkit._cdf_tk.resource_ios import RESOURCE_IO_BY_TYPE, ResourceIO, ResourceType, get_resource_build_io
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceFileIO, SuccessExtra
from cognite_toolkit._cdf_tk.sandbox._session import ModelingSession, ResourceRef
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename


@dataclass
class CdfScope:
    """Bounds a CDF read so the session does not pull an entire project."""

    kind: str
    folder: str | None = None
    data_set_external_id: str | None = None
    space: str | None = None
    identifiers: list[dict[str, Any]] | None = None


@dataclass
class KindPlan:
    kind: str
    folder: str
    to_create: list[str] = field(default_factory=list)
    to_update: list[str] = field(default_factory=list)
    to_delete: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "folder": self.folder,
            "toCreate": self.to_create,
            "toUpdate": self.to_update,
            "toDelete": self.to_delete,
            "unchanged": self.unchanged,
            "skipped": self.skipped,
            "unsupported": self.unsupported,
        }


@dataclass
class StorePlan:
    kinds: list[KindPlan] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {"kinds": [kind.to_json() for kind in self.kinds]}


@dataclass
class CommitResult:
    dry_run: bool
    results: list[DeploymentResult] = field(default_factory=list)
    written: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "dryRun": self.dry_run,
            "written": self.written,
            "results": [
                {
                    "resourceName": result.resource_name,
                    "created": result.created_count,
                    "updated": result.updated_count,
                    "deleted": result.deleted_count,
                    "unchanged": result.unchanged_count,
                    "skipped": [
                        {"id": str(item.id), "code": item.code, "reason": item.reason} for item in result.skipped
                    ],
                    "createdIds": [str(item) for item in result.created_ids],
                    "updatedIds": [str(item) for item in result.updated_ids],
                    "deletedIds": [str(item) for item in result.deleted_ids],
                    "unchangedIds": [str(item) for item in result.unchanged_ids],
                }
                for result in self.results
            ],
        }


class ResourceStore(Protocol):
    def plan(self, session: ModelingSession) -> StorePlan: ...

    def commit(self, session: ModelingSession, *, dry_run: bool = True, confirm: bool = False) -> CommitResult: ...


class YamlStore:
    """Pending session edits as unified diffs, and write them back to disk."""

    def plan(self, session: ModelingSession) -> StorePlan:
        diffs = session.diff()
        kind = KindPlan(
            kind="files",
            folder=".",
            to_update=[str(path) for path in diffs],
            to_delete=[str(path) for path in session.removed_paths()],
        )
        return StorePlan(kinds=[kind] if kind.to_update or kind.to_delete else [])

    def commit(self, session: ModelingSession, *, dry_run: bool = True, confirm: bool = False) -> CommitResult:
        if dry_run:
            plan = self.plan(session)
            return CommitResult(
                dry_run=True,
                written=[*plan.kinds[0].to_update, *plan.kinds[0].to_delete] if plan.kinds else [],
            )
        if not confirm:
            raise ValueError("Pass confirm=true to write pending edits to disk")
        return CommitResult(dry_run=False, written=[str(path) for path in session.write()])


class CdfStore:
    """Compare and deploy the session against CDF through ResourceIO."""

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    def plan(self, session: ModelingSession) -> StorePlan:
        return StorePlan(kinds=self._plan_kinds(session))

    def commit(self, session: ModelingSession, *, dry_run: bool = True, confirm: bool = False) -> CommitResult:
        if not dry_run and not confirm:
            raise ValueError("Pass confirm=true to deploy pending edits to CDF")
        steps, _ = self._deployment_steps(session)
        if not steps:
            return CommitResult(dry_run=dry_run)
        results = DeployV2Command(print_warning=False, skip_tracking=True, silent=True).apply_plan(
            self.client,
            steps,
            DeployOptions(operation="deploy", dry_run=dry_run, verbose=False),
        )
        return CommitResult(dry_run=dry_run, results=list(results))

    def read(self, scope: CdfScope) -> list[tuple[type[ResourceIO], dict[str, Any], dict[str, str]]]:
        """Read resources from CDF. Returns (crud_cls, source-format dict, sidecar text by field)."""
        crud_cls = self._crud_class(scope.kind, scope.folder)
        crud = crud_cls.create_io(self.client)
        if scope.identifiers:
            ids = [crud_cls.get_id(item) for item in scope.identifiers]
            responses = list(crud.retrieve(ids))
        else:
            responses = list(
                crud.iterate(data_set_external_id=scope.data_set_external_id, space=scope.space)
            )
        dumped: list[tuple[type[ResourceIO], dict[str, Any], dict[str, str]]] = []
        for response in responses:
            body = crud.dump_resource(response)
            if issubclass(crud_cls, ResourceFileIO):
                body = crud_cls.from_crud_type(body)
            sidecars: dict[str, str] = {}
            # split_resource expects a path only to name the sidecar; the YAML body is what we keep.
            base = Path(f"{sanitize_filename(str(crud.get_id(response)))}.{crud_cls.kind}.yaml")
            yaml_body: dict[str, Any] = body
            for path, content in crud.split_resource(base, deepcopy(body)):
                if path == base and isinstance(content, dict):
                    yaml_body = content
                elif isinstance(content, str):
                    # Convention: .sql next to a transformation is the query field.
                    field = "query" if path.suffix == ".sql" else path.suffix.lstrip(".") or "extra"
                    sidecars[field] = content
            dumped.append((crud_cls, yaml_body, sidecars))
        return dumped

    def merge_into_session(
        self,
        session: ModelingSession,
        kind: str | None = None,
        identifier: dict[str, Any] | None = None,
        folder: str | None = None,
    ) -> dict[str, Any]:
        """Merge CDF values into loaded session resources, keeping variables and comments."""
        refs = [
            ref
            for ref in session.resources()
            if (kind is None or ref.source.read.source_type.kind.lower() == kind.lower())
            and (folder is None or ref.source.read.source_type.resource_folder == folder)
            and (identifier is None or ref.resource.identifier.dump() == identifier)
        ]
        updated: list[dict[str, Any]] = []
        skipped: list[dict[str, str]] = []
        for ref in refs:
            result = self._merge_one(session, ref)
            if result is None:
                skipped.append(
                    {
                        "kind": ref.source.read.source_type.kind,
                        "identifier": str(ref.resource.identifier),
                        "reason": "Resource missing in CDF or kind has no CDF IO",
                    }
                )
            elif result:
                updated.append(
                    {
                        "kind": ref.source.read.source_type.kind,
                        "identifier": ref.resource.identifier.dump(),
                    }
                )
        return {"updated": updated, "skipped": skipped}

    def _merge_one(self, session: ModelingSession, ref: ResourceRef) -> bool | None:
        crud_cls = ref.source.crud_cls
        if not issubclass(crud_cls, ResourceIO):
            return None
        crud = crud_cls.create_io(self.client)
        retrieved = crud.retrieve([ref.resource.identifier])
        if not retrieved:
            return None

        local_built = self._local_built_dict(ref)
        cdf_dumped = crud.dump_resource(retrieved[0], local_built)
        if cdf_dumped == local_built:
            return False

        source_format = _to_source_format(ref.source.read.source_type, cdf_dumped)

        extras_to_set: dict[str, str] = {}
        for extra in ref.resource.extra_files:
            if not isinstance(extra, SuccessExtra) or not extra.resource_field:
                continue
            if extra.resource_field in source_format:
                value = source_format.pop(extra.resource_field)
                if isinstance(value, str):
                    extras_to_set[extra.resource_field] = value

        # split_resource may still peel sidecars off (e.g. Skill).
        base = ref.source.path
        for path, content in crud.split_resource(base, deepcopy(source_format)):
            if path == base and isinstance(content, dict):
                source_format = content
            elif isinstance(content, str) and path.suffix == ".sql":
                extras_to_set.setdefault("query", content)

        replacer = ResourceReplacer(ref.source.variable_by_placeholder, crud)
        merged_placeholder = replacer.replace(
            deepcopy(ref.source.resolved[ref.index]),
            deepcopy(ref.source.placeholder[ref.index]),
            source_format,
        )
        merged_resolved = _resolve_placeholders(merged_placeholder, ref.source.variable_by_placeholder)
        session.replace_resource(ref, merged_resolved, merged_placeholder)
        for field_name, text in extras_to_set.items():
            # Keep {{ variable }} tokens in sidecar text when the resolved value matches.
            session.set_extra(ref, field_name, _restore_sidecar_tokens(text, ref.source.variables))
        return True

    @staticmethod
    def _local_built_dict(ref: ResourceRef) -> dict[str, Any]:
        body = deepcopy(ref.source.resolved[ref.index])
        for extra in ref.resource.extra_files:
            if isinstance(extra, SuccessExtra) and extra.resource_field and extra.content is not None:
                body[extra.resource_field] = extra.content
                for remove in extra.remove_fields:
                    body.pop(remove, None)
        return body

    def _plan_kinds(self, session: ModelingSession) -> list[KindPlan]:
        by_crud = self._resources_by_crud(session)
        kinds: list[KindPlan] = []
        for crud_cls, resource_by_id in by_crud.items():
            crud = crud_cls.create_io(self.client)
            cdf_by_id = {crud.get_id(item): item for item in crud.retrieve(list(resource_by_id.keys()))}
            deployed = DeployV2Command.categorize_resources(
                crud,
                resource_by_id,
                cdf_by_id,
                console=self.client.console,
                options=DeployOptions(operation="deploy", dry_run=True, verbose=False),
            )
            for removed_id in session.removed_identifiers(crud_cls):
                if removed_id in cdf_by_id and removed_id not in deployed.to_delete:
                    deployed.to_delete.append(removed_id)
            kinds.append(self._kind_plan(crud_cls, deployed))
        for kind, folder, identifiers in session.unsupported_cdf_resources():
            kinds.append(
                KindPlan(kind=kind, folder=folder, unsupported=[str(item) for item in identifiers])
            )
        return kinds

    def _deployment_steps(self, session: ModelingSession) -> tuple[list[DeploymentStep], list[KindPlan]]:
        by_crud = self._resources_by_crud(session)
        dependencies = {crud_cls: set(crud_cls.dependencies) & set(by_crud) for crud_cls in by_crud}
        ordered = list(TopologicalSorter(dependencies).static_order()) if by_crud else []
        steps: list[DeploymentStep] = []
        plans: list[KindPlan] = []
        for crud_cls in ordered:
            resource_by_id = by_crud[crud_cls]
            crud = crud_cls.create_io(self.client)
            requests = [read.request for read in resource_by_id.values()]
            steps.append(DeploymentStep(crud_cls=crud_cls, files=[], resource_requests=requests))
            cdf_by_id = {crud.get_id(item): item for item in crud.retrieve(list(resource_by_id.keys()))}
            deployed = DeployV2Command.categorize_resources(
                crud, resource_by_id, cdf_by_id, options=DeployOptions(dry_run=True)
            )
            plans.append(self._kind_plan(crud_cls, deployed))
        return steps, plans

    def _resources_by_crud(
        self, session: ModelingSession
    ) -> dict[type[ResourceIO], dict[Identifier, ReadResource]]:
        session.ensure_built()
        by_crud: dict[type[ResourceIO], dict[Identifier, ReadResource]] = defaultdict(dict)
        for built in session.built_modules():
            for resource in built.resources:
                if not resource.can_verify:
                    continue
                crud_cls = resource.crud_cls
                if not issubclass(crud_cls, ResourceIO):
                    continue
                crud = crud_cls.create_io(self.client)
                raw = deepcopy(resource.read_built())
                try:
                    request = crud.load_resource(deepcopy(raw), is_dry_run=True)
                except Exception:
                    continue
                identifier = crud.get_id(request)
                by_crud[crud_cls][identifier] = ReadResource(
                    request=request,
                    raw_dict=raw,
                    source_files=[resource.source_path],
                )
        return by_crud

    @staticmethod
    def _kind_plan(crud_cls: type[ResourceIO], deployed: ResourceToDeploy) -> KindPlan:
        return KindPlan(
            kind=crud_cls.kind,
            folder=crud_cls.folder_name,
            to_create=[str(crud_cls.get_id(item)) for item in deployed.to_create],
            to_update=[str(crud_cls.get_id(item)) for item in deployed.to_update],
            to_delete=[str(item) for item in deployed.to_delete],
            unchanged=[str(item) for item in deployed.unchanged],
            skipped=[{"id": str(item.id), "code": item.code, "reason": item.reason} for item in deployed.skipped],
        )

    @staticmethod
    def _crud_class(kind: str, folder: str | None) -> type[ResourceIO]:
        matches = [
            io_cls
            for resource_type, io_cls in RESOURCE_IO_BY_TYPE.items()
            if io_cls.kind.lower() == kind.lower() and (folder is None or resource_type.resource_folder == folder)
        ]
        # RESOURCE_IO_BY_TYPE keys are ResourceType; values may repeat across aliases.
        unique = list({id(cls): cls for cls in matches}.values())
        if len(unique) != 1:
            options = sorted({cls.folder_name for cls in unique})
            raise ValueError(
                f"Unknown CDF kind {kind!r}" if not unique else f"{kind!r} exists in {options}, pass a folder"
            )
        return unique[0]


def _to_source_format(source_type: ResourceType | None, crud: dict[str, Any]) -> dict[str, Any]:
    if source_type is None:
        return crud
    io_cls = get_resource_build_io(source_type.resource_folder, source_type.kind)
    if not issubclass(io_cls, ResourceFileIO):
        return crud
    return io_cls.from_crud_type(crud)


def _resolve_placeholders(node: Any, variable_by_placeholder: dict[str, BuildVariable]) -> Any:
    """Turn VARIABLE_xxx placeholders back into resolved variable values."""
    if isinstance(node, dict):
        resolved: dict[str, Any] = {}
        for key, value in node.items():
            new_key = key
            if isinstance(key, str):
                for placeholder, variable in variable_by_placeholder.items():
                    if key == placeholder:
                        new_key = variable.value
                        break
            resolved[new_key] = _resolve_placeholders(value, variable_by_placeholder)
        return resolved
    if isinstance(node, list):
        return [_resolve_placeholders(item, variable_by_placeholder) for item in node]
    if isinstance(node, str):
        result = node
        for placeholder, variable in variable_by_placeholder.items():
            if placeholder in result:
                result = result.replace(placeholder, str(variable.value))
        return result
    return node


def _restore_sidecar_tokens(text: str, variables: list[BuildVariable]) -> str:
    """Put ``{{ name }}`` back into sidecar text when the resolved value is present."""
    restored = text
    for variable in variables:
        value = variable.value
        if isinstance(value, str) and value and value in restored:
            restored = restored.replace(value, f"{{{{ {variable.name} }}}}")
    return restored

