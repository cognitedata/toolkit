from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import AppConfigAcl, Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigViewProperty,
    SearchConfigWrite,
    SearchConfigWriteList,
    ViewId,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.resource_classes import SearchConfigYAML
from cognite_toolkit._cdf_tk.utils import quote_int_value_by_key_in_yaml, safe_read
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_identifiable, dm_identifier

from .datamodel_loaders import ViewLoader


@final
class SearchConfigLoader(
    ResourceLoader[ViewId, SearchConfigWrite, SearchConfig, SearchConfigWriteList, SearchConfigList]
):
    support_drop = False
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
    def get_id(cls, item: SearchConfig | SearchConfigWrite | dict) -> ViewId:
        if isinstance(item, dict):
            return ViewId.load(item.get("view", {}))
        return item.view

    @classmethod
    def dump_id(cls, id: ViewId) -> dict[str, Any]:
        return {"view": id.dump()}

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

    def _update_config_layout(
        self, existing_layout: list[SearchConfigViewProperty], new_layout: list[SearchConfigViewProperty]
    ) -> None:
        """
        Update the existing Coulmn/Filter/Property layout with the new layout for the given view.
        """
        index_by_property = {p.property: i for i, p in enumerate(existing_layout)}
        for prop in new_layout:
            if prop.property in index_by_property:
                existing_layout[index_by_property[prop.property]] = prop
            else:
                existing_layout.append(prop)
                index_by_property[prop.property] = len(existing_layout) - 1

    def create(self, items: SearchConfigWrite | SearchConfigWriteList) -> SearchConfigList:
        """Create new search configurations using the upsert method"""
        if isinstance(items, SearchConfigWrite):
            items = SearchConfigWriteList([items])

        result = SearchConfigList([])
        for item in items:
            created = self.client.search.configurations.upsert(item)
            result.append(created)
        return result

    def retrieve(self, ids: SequenceNotStr[ViewId]) -> SearchConfigList:
        """Retrieve search configurations by their IDs"""
        all_configs = self.client.search.configurations.list()
        # The API does not support server-side filtering, so we filter in memory.
        return SearchConfigList([config for config in all_configs if config.view in ids])

    def update(self, items: SearchConfigWrite | SearchConfigWriteList) -> SearchConfigList:
        """
        Update search configurations using the upsert method
        Note: Due to an API limitation where multiple configs can be created for the same view,
        this method merges all updates for the same view into a single configuration to prevent
        duplicate search configs per view.
        """
        if isinstance(items, SearchConfigWrite):
            items = SearchConfigWriteList([items])

        result = SearchConfigList([])
        view_ids = [item.view for item in items]
        existing_configs = self.retrieve(view_ids)
        existing_by_view_id = {config.view: config for config in existing_configs}

        for item in items:
            existing_config = existing_by_view_id.get(item.view)
            if existing_config is None:
                result.extend(self.create(item))
                continue

            columns = list(existing_config.columns_layout or [])
            filters = list(existing_config.filter_layout or [])
            props = list(existing_config.properties_layout or [])

            self._update_config_layout(columns, item.columns_layout or [])
            self._update_config_layout(filters, item.filter_layout or [])
            self._update_config_layout(props, item.properties_layout or [])

            use_as_name = item.use_as_name or existing_config.use_as_name
            use_as_description = item.use_as_description or existing_config.use_as_description

            merged_config = SearchConfigWrite(
                id=existing_config.id,  # Keep existing ID
                view=existing_config.view,  # Keep existing view
                use_as_name=use_as_name,
                use_as_description=use_as_description,
                columns_layout=columns if columns else None,
                filter_layout=filters if filters else None,
                properties_layout=props if props else None,
            )

            updated = self.client.search.configurations.upsert(merged_config)
            result.append(updated)

        return result

    def delete(self, ids: SequenceNotStr[ViewId]) -> int:
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
            return iter([])

        if data_set_external_id or parent_ids:
            # These filters are not supported for SearchConfig
            return iter([])

        return iter(all_configs)
