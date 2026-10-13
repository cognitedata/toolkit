"""Load several modules of an organization directory into one modeling session."""

from collections import Counter
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ExternalId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException
from cognite_toolkit._cdf_tk.sandbox import _operations
from cognite_toolkit._cdf_tk.sandbox._session import ApplyResult, ModelingSession

OPERATIONS: dict[str, type[_operations.Operation]] = {
    cls.__name__: cls for cls in _operations.Operation.__subclasses__()
}
DATA_MODELING_FOLDERS = ("data_modeling", "transformations")


def insight_to_json(insight: Insight | InternalValidatorException) -> dict[str, Any]:
    if isinstance(insight, InternalValidatorException):
        return {"insightType": type(insight).__name__, "message": str(insight)}
    return insight.model_dump(mode="json")


def parse_operation(payload: dict[str, Any]) -> _operations.Operation:
    payload = dict(payload)
    name = payload.pop("op", None)
    if name not in OPERATIONS:
        raise ValueError(f"Unknown op {name!r}. Available: {', '.join(sorted(OPERATIONS))}")
    return OPERATIONS[name].model_validate(payload)


class Workspace:
    """The session behind the server. Modules can be added by repeated loads."""

    def __init__(self) -> None:
        self.session: ModelingSession | None = None
        self._loaded_modules: set[Path] = set()

    def get_session(self) -> ModelingSession:
        if self.session is None:
            raise ValueError("Nothing is loaded. POST /load first.")
        return self.session

    def load(self, organization_dir: Path, config_yaml: Path | None, modules: list[str]) -> dict[str, Any]:
        organization_dir = organization_dir.resolve()
        build = BuildV2Command._read_file_system(organization_dir, config_yaml, modules)
        if build.selected_modules is None:
            raise ValueError("No modules selected. Pass modules or set environment.selected in the config YAML.")
        scan = BuildV2Command._find_modules(build, "load")

        if self.session is None or self.session.root != organization_dir:
            self.session = ModelingSession(root=organization_dir)
            self._loaded_modules = set()

        loaded: list[str] = []
        for module in scan.modules:
            if module.path in self._loaded_modules:
                continue
            for folder in DATA_MODELING_FOLDERS:
                if (module.path / folder).is_dir():
                    self.session.load_dir(module.path / folder, module.variables)
            self._loaded_modules.add(module.path)
            loaded.append(module.id.as_posix())
        return {"modules": loaded, "counts": self.counts()}

    def counts(self) -> dict[str, int]:
        session = self.get_session()
        counts = Counter(
            type(request).__name__.removesuffix("Request").lower() for request in session.requests.values()
        )
        counts["transformationQuery"] = len(session.queries)
        return dict(counts)

    def list_resources(self) -> list[dict[str, Any]]:
        session = self.get_session()
        resources = [
            {
                "kind": type(request).__name__.removesuffix("Request").lower(),
                "identifier": identifier.dump(),
                "file": str(session._locations[identifier].path),
            }
            for identifier, request in session.requests.items()
        ]
        resources.extend(
            {"kind": "transformationquery", "identifier": identifier.dump(), "file": str(source.path)}
            for identifier, source in session.queries.items()
        )
        return resources

    def get_resource(self, kind: str, space: str | None, external_id: str | None, version: str | None) -> Any:
        session = self.get_session()
        if kind == "transformationquery":
            return {"query": session.get_query(ExternalId(external_id=_required(external_id, "externalId"))).text}
        identifier: Identifier
        if kind == "space":
            identifier = SpaceId(space=_required(space, "space"))
        elif kind == "container":
            identifier = ContainerId(space=_required(space, "space"), external_id=_required(external_id, "externalId"))
        elif kind == "view":
            identifier = ViewId(
                space=_required(space, "space"),
                external_id=_required(external_id, "externalId"),
                version=_required(version, "version"),
            )
        elif kind == "datamodel":
            identifier = DataModelId(
                space=_required(space, "space"),
                external_id=_required(external_id, "externalId"),
                version=_required(version, "version"),
            )
        else:
            raise ValueError(f"Unknown kind {kind!r}")
        return session.get(identifier).dump()

    def apply(self, payload: dict[str, Any]) -> ApplyResult:
        return self.get_session().apply(parse_operation(payload))


def _required(value: str | None, name: str) -> str:
    if not value:
        raise ValueError(f"Missing query parameter {name!r}")
    return value
