from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import AppConfigAcl, Capability

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewReferenceNoVersion
from cognite_toolkit._cdf_tk.client.resource_classes.search_config import SearchConfigRequest, SearchConfigResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import SearchConfigYAML
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_identifiable, dm_identifier

from .datamodel import ViewCRUD


@final
class SearchConfigCRUD(ResourceCRUD[ViewReferenceNoVersion, SearchConfigRequest, SearchConfigResponse]):
    support_drop = False
    folder_name = "cdf_applications"
    resource_cls = SearchConfigResponse
    resource_write_cls = SearchConfigRequest
    yaml_cls = SearchConfigYAML
    dependencies = frozenset({ViewCRUD})
    kind = "SearchConfig"
    _doc_base_url = "https://api-docs.cogheim.net/redoc/#tag/"
    _doc_url = "Search-Config/operation/upsertSearchConfigViews"

    def __init__(self, client: ToolkitClient, build_path: Path | None, console: Console | None):
        super().__init__(client, build_path, console)
        self._internal_id_by_view: dict[ViewReferenceNoVersion, int] | None = None

    @property
    def display_name(self) -> str:
        return "search config"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SearchConfigRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [AppConfigAcl.Action.Read] if read_only else [AppConfigAcl.Action.Read, AppConfigAcl.Action.Write]

        return AppConfigAcl(
            actions=actions,
            scope=AppConfigAcl.Scope.AppConfig(apps=["SEARCH"]),
            allow_unknown=True,
        )

    @classmethod
    def get_id(cls, item: SearchConfigRequest | SearchConfigResponse | dict) -> ViewReferenceNoVersion:
        if isinstance(item, dict):
            view = item.get("view", {})
            return ViewReferenceNoVersion(space=view.get("space", ""), external_id=view.get("externalId", ""))
        return ViewReferenceNoVersion(space=item.view.space, external_id=item.view.external_id)

    @classmethod
    def dump_id(cls, id: ViewReferenceNoVersion) -> dict[str, Any]:
        return {"view": id.dump()}

    @classmethod
    def as_str(cls, id: ViewReferenceNoVersion) -> str:
        return sanitize_filename(f"{id.external_id}_{id.space}")

    def dump_resource(self, resource: SearchConfigResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        dumped.pop("id", None)
        for key in ["columnsLayout", "filterLayout", "propertiesLayout"]:
            if not dumped.get(key) and key not in local:
                dumped.pop(key, None)
        return dumped

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SearchConfigRequest:
        loaded = SearchConfigRequest._load(resource)

        if self._internal_id_by_view is None:
            all_configs = self.client.tool.search_configurations.list()
            self._internal_id_by_view = {config.view: config.id for config in all_configs}

        if loaded.view in self._internal_id_by_view:
            loaded.id = self._internal_id_by_view[loaded.view]

        return loaded

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path in [("columnsLayout",), ("filterLayout",), ("propertiesLayout",)]:
            return diff_list_identifiable(local, cdf, get_identifier=lambda x: x.get("property"))
        elif json_path == ("view",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    def create(self, items: Sequence[SearchConfigRequest]) -> list[SearchConfigResponse]:
        """
        Create new search configurations using the create/upsert method
        """
        return self.client.tool.search_configurations.create(items)

    def retrieve(self, ids: Sequence[ViewReferenceNoVersion]) -> list[SearchConfigResponse]:
        """Retrieve search configurations by their IDs"""
        all_configs = self.client.tool.search_configurations.list()
        id_set = set(ids)
        # The API does not support server-side filtering, so we filter in memory.
        return [config for config in all_configs if config.view in id_set]

    def update(self, items: Sequence[SearchConfigRequest]) -> list[SearchConfigResponse]:
        """
        Update search configurations using the update/upsert method
        """
        return self.client.tool.search_configurations.update(items)

    def delete(self, ids: Sequence[ViewReferenceNoVersion]) -> int:
        """
        Delete is not implemented in the API client
        """
        raise NotImplementedError("Delete operation is not supported for search config")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SearchConfigResponse]:
        """Iterate through all search configurations"""
        if space or data_set_external_id or parent_ids:
            # These filters are not supported for SearchConfig
            return iter([])

        all_configs = self.client.tool.search_configurations.list()
        return iter(all_configs)
