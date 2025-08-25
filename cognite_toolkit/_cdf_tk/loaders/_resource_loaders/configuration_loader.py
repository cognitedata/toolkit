from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import AppConfigAcl, Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigWrite,
    SearchConfigWriteList,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.resource_classes import SearchConfigYAML
from cognite_toolkit._cdf_tk.utils import quote_int_value_by_key_in_yaml, safe_read
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_identifiable, dm_identifier

from .datamodel_loaders import ViewLoader


@final
class SearchConfigLoader(ResourceLoader[str, SearchConfigWrite, SearchConfig, SearchConfigWriteList, SearchConfigList]):
    folder_name = "search_configs"
    filename_pattern = r"^.*SearchConfig$"
    resource_cls = SearchConfig
    resource_write_cls = SearchConfigWrite
    list_cls = SearchConfigList
    list_write_cls = SearchConfigWriteList
    yaml_cls = SearchConfigYAML
    dependencies = frozenset({ViewLoader})
    kind = "SearchConfig"
    _doc_base_url = "https://api-docs.cogheim.net/redoc/#tag/"
    _doc_url = "Search-Config/operation/upsertSearchConfigViews"

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
            scope=AppConfigAcl.Scope.All(),
            allow_unknown=True,
        )

    @classmethod
    def get_id(cls, item: SearchConfig | SearchConfigWrite | dict) -> str:
        if isinstance(item, dict):
            return str(item.get("id", ""))
        if isinstance(item, SearchConfig) and not item.id:
            raise KeyError("SearchConfig must have id")
        return str(item.id) if item.id is not None else ""

    @classmethod
    def dump_id(cls, id: int | str) -> dict[str, Any]:
        return {"id": id}

    def safe_read(self, filepath: Path | str) -> str:
        # Search config Id is int type
        return quote_int_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="id")

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SearchConfigWrite:
        if "id" in resource and isinstance(resource["id"], str) and resource["id"].isdigit():
            resource["id"] = int(resource["id"])

        return self.resource_write_cls._load(resource)

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path in [("columnsLayout",), ("filterLayout",), ("propertiesLayout",)]:
            return diff_list_identifiable(local, cdf, get_identifier=lambda x: x.get("property"))
        elif json_path == ("view",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    def create(self, items: SearchConfigWrite | SearchConfigWriteList) -> SearchConfigList:
        """Create new search configurations using the upsert method"""
        if isinstance(items, SearchConfigWrite):
            items = SearchConfigWriteList([items])

        created: list[SearchConfig] = []
        for item in items:
            response = self.client.search.configurations.upsert(item)
            if isinstance(response, SearchConfigList):
                created.extend(response)
            else:
                created.append(response)

        return SearchConfigList(created)

    def retrieve(self, ids: SequenceNotStr[str]) -> SearchConfigList:
        """Retrieve search configurations by their IDs"""
        # TODO: Raise warning if ids are not numeric
        numeric_ids = [int(id_str) for id_str in ids if id_str.isdigit()]

        all_configs = self.client.search.configurations.list()
        # The API does not support server-side filtering, so we filter in memory.
        return SearchConfigList([config for config in all_configs if config.id in numeric_ids])

    def update(self, items: SearchConfigWrite | SearchConfigWriteList) -> SearchConfigList:
        """Update search configurations using the upsert method"""
        if isinstance(items, SearchConfigWrite):
            items = SearchConfigWriteList([items])

        if any([item for item in items if not item.id]):
            raise KeyError("Search Configuration Update Requires Id!")
        return self.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        """
        Delete is not implemented in the API client
        """
        raise NotImplementedError("Delete operation is not supported for SearchConfig")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SearchConfig]:
        """Iterate through all search configurations"""
        all_configs = self.client.search.configurations.list()
        if space:
            # The API does not support server-side filtering, so we filter in memory.
            return iter([config for config in all_configs if config.view.space == space])

        if data_set_external_id or parent_ids:
            # These filters are not supported for SearchConfig
            return iter([])

        return iter(all_configs)
