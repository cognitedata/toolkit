from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RuleSetVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.group import AclType, AllScope, ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset import RuleSetRequest, RuleSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset_version import (
    RuleSetVersionRequest,
    RuleSetVersionResponse,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.auth import GroupAllScopedCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.utils import load_yaml_inject_variables, safe_read, sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import RuleSetVersionYAML, RuleSetYAML


@final
class RuleSetCRUD(ResourceCRUD[ExternalId, RuleSetRequest, RuleSetResponse]):
    folder_name = "rulesets"
    resource_cls = RuleSetResponse
    resource_write_cls = RuleSetRequest
    kind = "RuleSet"
    yaml_cls = RuleSetYAML
    dependencies = frozenset({GroupAllScopedCRUD})
    support_drop = True
    support_update = False
    _doc_url = "Rule-sets/operation/createRuleSet"

    @property
    def display_name(self) -> str:
        return "rule sets"

    @classmethod
    def get_id(cls, item: RuleSetRequest | RuleSetResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return {"externalId": id.external_id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RuleSetRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    @classmethod
    def get_minimum_scope(cls, items: Sequence[RuleSetRequest]) -> ScopeDefinition | None:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        yield from ()

    def create(self, items: Sequence[RuleSetRequest]) -> list[RuleSetResponse]:
        return self.client.tool.rulesets.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[RuleSetResponse]:
        return self.client.tool.rulesets.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RuleSetRequest]) -> list[RuleSetResponse]:
        raise NotImplementedError("Rule sets do not support updates.")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.rulesets.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RuleSetResponse]:
        for items in self.client.tool.rulesets.iterate(limit=None):
            yield from items


@final
class RuleSetVersionCRUD(ResourceCRUD[RuleSetVersionId, RuleSetVersionRequest, RuleSetVersionResponse]):
    folder_name = "rulesets"
    resource_cls = RuleSetVersionResponse
    resource_write_cls = RuleSetVersionRequest
    kind = "RuleSetVersion"
    yaml_cls = RuleSetVersionYAML
    dependencies = frozenset({RuleSetCRUD})
    parent_resource = frozenset({RuleSetCRUD})
    support_drop = True
    support_update = False
    _doc_url = "Rule-sets/operation/createRuleSetVersions"

    @property
    def display_name(self) -> str:
        return "rule set versions"

    @classmethod
    def get_id(cls, item: RuleSetVersionRequest | RuleSetVersionResponse | dict) -> RuleSetVersionId:
        if isinstance(item, dict):
            ext_id = item.get("ruleSetExternalId") or item.get("rule_set_external_id")
            version = item.get("version")
            if ext_id is None:
                raise KeyError("ruleSetExternalId")
            if version is None:
                raise KeyError("version")
            return RuleSetVersionId(
                rule_set_external_id=ext_id,
                version=version,
            )
        return item.as_id()

    @classmethod
    def dump_id(cls, id: RuleSetVersionId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: RuleSetVersionId) -> str:
        return sanitize_filename(f"{id.rule_set_external_id}_v{id.version}")

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RuleSetVersionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    @classmethod
    def get_minimum_scope(cls, items: Sequence[RuleSetVersionRequest]) -> ScopeDefinition | None:
        return None

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        yield from ()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "ruleSetExternalId" in item:
            yield RuleSetCRUD, ExternalId(external_id=item["ruleSetExternalId"])

    @classmethod
    def get_dependencies(cls, resource: RuleSetVersionYAML) -> Iterable[tuple[type[ResourceCRUD], Identifier]]:
        yield RuleSetCRUD, ExternalId(external_id=resource.rule_set_external_id)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources = load_yaml_inject_variables(
            self.safe_read(filepath),
            environment_variables or {},
            original_filepath=filepath,
        )
        raw_list = resources if isinstance(resources, list) else [resources]

        for item in raw_list:
            rule_set_id = item.get("ruleSetExternalId", item.get("rule_set_external_id", filepath.stem))
            if "rules" in item:
                continue
            # Look for .ttl by convention: {stem}.ttl or {rule_set_external_id}.ttl
            ttl_candidates = [
                filepath.parent / f"{filepath.stem}.ttl",
                filepath.parent / f"{rule_set_id}.ttl",
            ]
            ttl_path = next((p for p in ttl_candidates if p.exists()), None)
            if ttl_path is None:
                raise ToolkitFileNotFoundError(
                    f"'rules' is missing and no .ttl file found. Expected one of: {[str(p) for p in ttl_candidates]}",
                    filepath,
                )
            content = safe_read(ttl_path, encoding=BUILD_FOLDER_ENCODING)
            item["rules"] = [content]

        return raw_list

    def split_resource(
        self, base_filepath: Path, resource: dict[str, Any]
    ) -> Iterable[tuple[Path, dict[str, Any] | str]]:
        """Write rules to a .ttl file only if one does not already exist."""
        rules = resource.pop("rules", None)
        ttl_path = base_filepath.with_suffix(".ttl")
        if rules:
            if ttl_path.exists():
                resource["rules"] = rules
            else:
                yield ttl_path, "\n\n".join(rules)
        yield base_filepath, resource

    def create(self, items: Sequence[RuleSetVersionRequest]) -> list[RuleSetVersionResponse]:
        if not items:
            return []
        return self.client.tool.rulesets.versions.create(list(items))

    def retrieve(self, ids: Sequence[RuleSetVersionId]) -> list[RuleSetVersionResponse]:
        if not ids:
            return []
        return self.client.tool.rulesets.versions.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RuleSetVersionRequest]) -> list[RuleSetVersionResponse]:
        raise NotImplementedError("Rule set versions are immutable and do not support updates.")

    def delete(self, ids: Sequence[RuleSetVersionId]) -> int:
        if not ids:
            return 0
        self.client.tool.rulesets.versions.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RuleSetVersionResponse]:
        if parent_ids is not None:
            rs_ext_ids = {
                pid.external_id if isinstance(pid, ExternalId) else pid
                for pid in parent_ids
                if isinstance(pid, (str, ExternalId))
            }
        else:
            rs_ext_ids = {rs.external_id for rs in self.client.tool.rulesets.list(limit=None)}

        for rs_ext_id in rs_ext_ids:
            for versions in self.client.tool.rulesets.versions.iterate(rs_ext_id, limit=None):
                yield from versions
