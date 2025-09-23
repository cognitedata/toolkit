from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import AppConfigAcl, Capability
from cognite.client.utils.useful_types import SequenceNotStr
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigWrite,
    SearchConfigWriteList,
    ViewId,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import SearchConfigYAML
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_identifiable, dm_identifier

from .datamodel import ViewCRUD


@final
class SearchConfigCRUD(ResourceCRUD[ViewId, SearchConfigWrite, SearchConfig, SearchConfigWriteList, SearchConfigList]):
    support_drop = False
    folder_name = "cdf_applications"
    filename_pattern = r"^.*SearchConfig$"
    resource_cls = SearchConfig
    resource_write_cls = SearchConfigWrite
    list_cls = SearchConfigList
    list_write_cls = SearchConfigWriteList
    yaml_cls = SearchConfigYAML
    dependencies = frozenset({ViewCRUD})
    kind = "SearchConfig"
    _doc_base_url = "https://api-docs.cogheim.net/redoc/#tag/"
    _doc_url = "Search-Config/operation/upsertSearchConfigViews"

    def __init__(self, client: ToolkitClient, build_path: Path | None, console: Console | None):
        super().__init__(client, build_path, console)
        self._existing_configs_cache: dict[ViewId, int] | None = None

    @property
    def display_name(self) -> str:
        return "search config"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SearchConfigWrite] | None, read_only: bool
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
    def get_id(cls, item: SearchConfig | SearchConfigWrite | dict) -> ViewId:
        if isinstance(item, dict):
            return ViewId.load(item.get("view", {}))
        return item.view

    @classmethod
    def dump_id(cls, id: ViewId) -> dict[str, Any]:
        return {"view": id.dump()}

    def dump_resource(self, resource: SearchConfig, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if "id" in dumped and "id" not in local:
            dumped.pop("id", None)
        for key in ["columnsLayout", "filterLayout", "propertiesLayout"]:
            if not dumped.get(key) and key not in local:
                dumped.pop(key, None)
        return dumped

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SearchConfigWrite:
        loaded = SearchConfigWrite._load(resource)

        if self._existing_configs_cache is None:
            all_configs = self.client.search.configurations.list()
            self._existing_configs_cache = {config.view: config.id for config in all_configs if config.id is not None}

        if loaded.view in self._existing_configs_cache:
            loaded.id = self._existing_configs_cache[loaded.view]

        return loaded

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path in [("columnsLayout",), ("filterLayout",), ("propertiesLayout",)]:
            return diff_list_identifiable(local, cdf, get_identifier=lambda x: x.get("property"))
        elif json_path == ("view",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    def _upsert(self, items: SearchConfigWrite | SearchConfigWriteList) -> SearchConfigList:
        """
        Upsert search configurations using the upsert method
        """
        if isinstance(items, SearchConfigWrite):
            items = SearchConfigWriteList([items])

        result = SearchConfigList([])
        for item in items:
            created = self.client.search.configurations.upsert(item)
            result.append(created)
        return result

    def create(self, items: SearchConfigWriteList) -> SearchConfigList:
        """
        Create new search configurations using the upsert method
        """
        return self._upsert(items)

    def retrieve(self, ids: SequenceNotStr[ViewId]) -> SearchConfigList:
        """Retrieve search configurations by their IDs"""
        all_configs = self.client.search.configurations.list()
        # The API does not support server-side filtering, so we filter in memory.
        return SearchConfigList([config for config in all_configs if config.view in ids])

    def update(self, items: SearchConfigWriteList) -> SearchConfigList:
        """
        Update search configurations using the upsert method
        """
        return self._upsert(items)

    def delete(self, ids: SequenceNotStr[ViewId]) -> int:
        """
        Delete is not implemented in the API client
        """
        raise NotImplementedError("Delete operation is not supported for search config")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SearchConfig]:
        """Iterate through all search configurations"""
        if space or data_set_external_id or parent_ids:
            # These filters are not supported for SearchConfig
            return iter([])

        all_configs = self.client.search.configurations.list()
        return iter(all_configs)
