from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, IntegrationConfigId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    IntegrationConfigsAcl,
    IntegrationsAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.integration import (
    IntegrationConfigRequest,
    IntegrationConfigResponse,
    IntegrationRequest,
    IntegrationResponse,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils import (
    load_yaml_inject_variables,
    read_yaml_content,
    safe_read,
    sanitize_filename,
    stringify_value_by_key_in_yaml,
)
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.yaml_classes import IntegrationConfigYAML, IntegrationYAML


def _external_id(item: dict[str, Any], resource_name: str) -> str:
    external_id = item.get("externalId") or item.get("external_id")
    if not isinstance(external_id, str) or not external_id:
        raise ToolkitRequiredValueError(f"{resource_name} must have externalId set.")
    return external_id


@final
class IntegrationsIO(ResourceIO[ExternalId, IntegrationRequest, IntegrationResponse, IntegrationYAML]):
    folder_name = "integrations"
    resource_cls = IntegrationResponse
    resource_write_cls = IntegrationRequest
    kind = "Integration"
    yaml_cls = IntegrationYAML
    dependencies = frozenset()
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Integrations/operation/createIntegrations"

    @property
    def display_name(self) -> str:
        return "integrations"

    @classmethod
    def get_id(cls, item: IntegrationRequest | IntegrationResponse | dict[str, Any]) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=_external_id(item, "Integration"))
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[IntegrationRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield IntegrationsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: IntegrationYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    def create(self, items: Sequence[IntegrationRequest]) -> list[IntegrationResponse]:
        return self.client.integrations.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[IntegrationResponse]:
        if not ids:
            return []
        return self.client.integrations.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[IntegrationRequest]) -> list[IntegrationResponse]:
        return self.client.integrations.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.integrations.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[IntegrationResponse]:
        if data_set_external_id or space or parent_ids:
            return iter(())
        return (item for page in self.client.integrations.iterate(limit=None) for item in page)


@final
class IntegrationConfigsIO(
    ResourceIO[ExternalId, IntegrationConfigRequest, IntegrationConfigResponse, IntegrationConfigYAML]
):
    folder_name = "integrations"
    resource_cls = IntegrationConfigResponse
    resource_write_cls = IntegrationConfigRequest
    kind = "IntegrationConfig"
    yaml_cls = IntegrationConfigYAML
    dependencies = frozenset({IntegrationsIO})
    parent_resource = frozenset({IntegrationsIO})
    # A new revision is created instead of updating the previous one.
    support_update = True
    # Revisions cannot be deleted. Deleting the integration removes them.
    support_drop = False
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Integration-Configuration/operation/createIntegrationConfig"

    @property
    def display_name(self) -> str:
        return "integration configs"

    @classmethod
    def get_id(cls, item: IntegrationConfigRequest | IntegrationConfigResponse | dict[str, Any]) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=_external_id(item, "Integration config"))
        if not item.external_id:
            raise ToolkitRequiredValueError("Integration config must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[IntegrationConfigRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield IntegrationConfigsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: IntegrationConfigYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        yield IntegrationsIO, ExternalId(external_id=resource.external_id)

    @classmethod
    def safe_read(cls, filepath: Path | str) -> str:
        # Config is a string on the API. Users write it as a mapping, so force a block scalar.
        return stringify_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="config")

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        # Environment variable keys inside config are preserved for the extractor to resolve.
        raw_str = self.safe_read(filepath)
        original = load_yaml_inject_variables(raw_str, {}, validate=False, original_filepath=filepath)
        replaced = load_yaml_inject_variables(
            raw_str, environment_variables or {}, validate=False, original_filepath=filepath
        )
        if isinstance(original, dict) and isinstance(replaced, dict):
            if "config" in original:
                replaced["config"] = original.get("config")
            return [replaced]
        if isinstance(original, list) and isinstance(replaced, list):
            for orig_item, repl_item in zip(original, replaced, strict=False):
                if isinstance(orig_item, dict) and isinstance(repl_item, dict) and "config" in orig_item:
                    repl_item["config"] = orig_item.get("config")
            return replaced
        return replaced if isinstance(replaced, list) else [replaced]

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> IntegrationConfigRequest:
        config_raw = resource.get("config")
        if isinstance(config_raw, dict):
            resource["config"] = yaml_safe_dump(config_raw, sort_keys=False)
        return IntegrationConfigRequest.model_validate(resource)

    def _get_id(self, resource: dict[str, Any], default: str) -> str:
        try:
            return str(self.get_id(resource))
        except (ToolkitRequiredValueError, KeyError):
            return default

    def dump_resource(self, resource: IntegrationConfigResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if (
            "config" in dumped
            and isinstance(dumped["config"], str)
            and ("config" not in local or isinstance(local["config"], dict))
        ):
            if dumped["config"].strip() == "":
                dumped["config"] = {}
            else:
                dumped["config"] = read_yaml_content(dumped["config"])
        return dumped

    def create(self, items: Sequence[IntegrationConfigRequest]) -> list[IntegrationConfigResponse]:
        return self.client.integrations.configuration.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[IntegrationConfigResponse]:
        if not ids:
            return []
        return self.client.integrations.configuration.retrieve(
            [IntegrationConfigId(external_id=id_.external_id) for id_ in ids],
            ignore_unknown_ids=True,
        )

    def update(self, items: Sequence[IntegrationConfigRequest]) -> list[IntegrationConfigResponse]:
        # The API appends a revision. Creating is how a configuration is changed.
        return self.create(items)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        """Config revisions cannot be deleted.

        Deleting the parent integration removes its revisions. This counts revisions that exist so callers
        can report how many would disappear with the integration.
        """
        count = 0
        for id_ in ids:
            try:
                result = self.client.integrations.configuration.list(
                    integration_external_id=id_.external_id, limit=None
                )
            except ToolkitAPIError as e:
                if e.code == 404:
                    continue
                raise
            else:
                count += len(result)
        return count

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[IntegrationConfigResponse]:
        if data_set_external_id or space:
            return
        if parent_ids is None:
            parent_external_ids: Iterable[ExternalId] = (
                integration.as_id() for page in self.client.integrations.iterate(limit=None) for integration in page
            )
        else:
            parent_external_ids = [pid for pid in parent_ids if isinstance(pid, ExternalId)]
        for parent_id in parent_external_ids:
            try:
                yield from self.client.integrations.configuration.retrieve(
                    [IntegrationConfigId(external_id=parent_id.external_id)],
                    ignore_unknown_ids=True,
                )
            except ToolkitAPIError as e:
                if e.code == 404:
                    continue
                raise
