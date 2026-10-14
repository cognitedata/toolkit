"""Load several modules of an organization directory into one modeling session."""

from collections import Counter
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException
from cognite_toolkit._cdf_tk.sandbox._operations import (
    AddResource,
    Operation,
    RemovePath,
    RemoveResource,
    SetExtra,
    SetPath,
)
from cognite_toolkit._cdf_tk.sandbox._session import ApplyResult, ModelingSession
from cognite_toolkit._cdf_tk.sandbox._store import CdfScope, CdfStore, CommitResult, StorePlan, YamlStore

OPERATIONS: dict[str, type[Operation]] = {
    cls.__name__: cls for cls in (SetPath, RemovePath, SetExtra, AddResource, RemoveResource)
}


def insight_to_json(insight: Insight | InternalValidatorException) -> dict[str, Any]:
    if isinstance(insight, InternalValidatorException):
        return {"insightType": type(insight).__name__, "message": str(insight)}
    return insight.model_dump(mode="json")


def parse_operation(payload: dict[str, Any]) -> Operation:
    payload = dict(payload)
    name = payload.pop("op", None)
    if name not in OPERATIONS:
        raise ValueError(f"Unknown op {name!r}. Available: {', '.join(sorted(OPERATIONS))}")
    return OPERATIONS[name].model_validate(payload)


class Workspace:
    """The session behind the server. Modules can be added by repeated loads."""

    def __init__(self, client: ToolkitClient | None = None) -> None:
        self.client = client
        self.session: ModelingSession | None = None
        self._loaded_modules: set[Path] = set()
        self.yaml_store = YamlStore()
        self._cdf_store: CdfStore | None = CdfStore(client) if client is not None else None

    @property
    def cdf_store(self) -> CdfStore:
        if self._cdf_store is None:
            raise ValueError("No CDF client. Start the sandbox with credentials (for example --env-file).")
        return self._cdf_store

    def get_session(self) -> ModelingSession:
        if self.session is None:
            raise ValueError("Nothing is loaded. POST /load first.")
        return self.session

    def load(self, organization_dir: Path, config_yaml: Path | None, modules: list[str]) -> dict[str, Any]:
        organization_dir = organization_dir.resolve()
        build = BuildV2Command.read_file_system(organization_dir, config_yaml, modules)
        if build.selected_modules is None:
            raise ValueError("No modules selected. Pass modules or set environment.selected in the config YAML.")
        scan = BuildV2Command.find_modules(build, "load")
        if scan.non_existing_module_names:
            raise ValueError(f"Modules do not exist: {[item.name for item in scan.non_existing_module_names]}")

        if self.session is None or self.session.root != organization_dir:
            self.session = ModelingSession(root=organization_dir, client=self.client)
            self._loaded_modules = set()

        loaded: list[str] = []
        for module in scan.modules:
            if module.path in self._loaded_modules:
                continue
            self.session.add_module(module)
            self._loaded_modules.add(module.path)
            loaded.append(module.id.as_posix())
        return {"modules": loaded, "counts": self.counts()}

    def counts(self) -> dict[str, int]:
        session = self.get_session()
        return dict(Counter(ref.source.read.source_type.kind.lower() for ref in session.resources()))

    def list_resources(self) -> list[dict[str, Any]]:
        return [
            {
                "kind": ref.source.read.source_type.kind,
                "folder": ref.source.read.source_type.resource_folder,
                "identifier": ref.resource.identifier.dump(),
                "file": str(ref.source.path),
            }
            for ref in self.get_session().resources()
        ]

    def get_resource(self, kind: str, identifier: dict[str, Any], folder: str | None = None) -> dict[str, Any]:
        session = self.get_session()
        ref = session.find(kind, identifier, folder)
        return {
            "resource": ref.source.resolved[ref.index],
            "extraFiles": [
                {"field": field, "file": str(text_file.path), "text": text_file.text}
                for field, text_file in session.extra_texts(ref).items()
            ],
        }

    def get_schema(self, kind: str, folder: str | None = None) -> dict[str, Any]:
        crud_cls = self.get_session().crud_class(kind, folder)
        return TypeAdapter(crud_cls.yaml_cls).json_schema(by_alias=True)

    def apply(self, payload: dict[str, Any]) -> ApplyResult:
        return self.get_session().apply(parse_operation(payload))

    def yaml_plan(self) -> StorePlan:
        return self.yaml_store.plan(self.get_session())

    def yaml_commit(self, *, dry_run: bool = False, confirm: bool = True) -> CommitResult:
        # /write keeps the old behaviour: write for real without an extra confirm flag.
        return self.yaml_store.commit(self.get_session(), dry_run=dry_run, confirm=confirm)

    def cdf_plan(self) -> StorePlan:
        return self.cdf_store.plan(self.get_session())

    def cdf_deploy(self, *, dry_run: bool = True, confirm: bool = False) -> CommitResult:
        return self.cdf_store.commit(self.get_session(), dry_run=dry_run, confirm=confirm)

    def cdf_load(self, body: dict[str, Any]) -> dict[str, Any]:
        """Load resources from CDF into the session as new files (dump)."""
        session = self.get_session()
        scope = CdfScope(
            kind=body["kind"],
            folder=body.get("folder"),
            data_set_external_id=body.get("dataSetExternalId"),
            space=body.get("space"),
            identifiers=body.get("identifiers"),
        )
        if not scope.identifiers and not scope.data_set_external_id and not scope.space:
            raise ValueError("Pass identifiers, dataSetExternalId, or space to bound the CDF load")
        module = body.get("module")
        added: list[dict[str, Any]] = []
        for crud_cls, resource, sidecars in self.cdf_store.read(scope):
            identifier = session.add_resource(
                crud_cls.kind,
                resource,
                folder=crud_cls.folder_name,
                module=module,
                extras=sidecars or None,
                from_cdf=True,
            )
            added.append({"kind": crud_cls.kind, "identifier": identifier.dump()})
        return {"added": added, "counts": self.counts()}

    def cdf_pull(self, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Merge CDF values into resources already loaded in the session."""
        body = body or {}
        return self.cdf_store.merge_into_session(
            self.get_session(),
            kind=body.get("kind"),
            identifier=body.get("identifier"),
            folder=body.get("folder"),
        )
