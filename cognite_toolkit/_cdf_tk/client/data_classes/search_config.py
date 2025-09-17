from abc import ABC
from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from typing_extensions import Self


@dataclass(frozen=True)
class ViewId(CogniteObject):
    external_id: str
    space: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            space=resource["space"],
        )


@dataclass
class SearchConfigViewProperty(CogniteObject):
    property: str
    disabled: bool | None = None
    selected: bool | None = None
    hidden: bool | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            property=resource["property"],
            disabled=resource.get("disabled"),
            selected=resource.get("selected"),
            hidden=resource.get("hidden"),
        )


class SearchConfigCore(WriteableCogniteResource["SearchConfigWrite"], ABC):
    """
    Core model for a single Configuration.

    Args:
        view: The configuration for one specific view.
        id: A server-generated ID for the object.
        use_as_name: The name of property to use for the name column in the UI.
        use_as_description: The name of property to use for the description column in the UI.
        columns_layout: Array of column configurations per property.
        filter_layout: Array of filter configurations per property.
        properties_layout: Array of property configurations per property.
    """

    def __init__(
        self,
        view: ViewId,
        id: int | None = None,
        use_as_name: str | None = None,
        use_as_description: str | None = None,
        columns_layout: list[SearchConfigViewProperty] | None = None,
        filter_layout: list[SearchConfigViewProperty] | None = None,
        properties_layout: list[SearchConfigViewProperty] | None = None,
    ) -> None:
        self.view = view
        self.id = id
        self.use_as_name = use_as_name
        self.use_as_description = use_as_description
        self.columns_layout = columns_layout
        self.filter_layout = filter_layout
        self.properties_layout = properties_layout

    def as_write(self) -> "SearchConfigWrite":
        return SearchConfigWrite(
            view=self.view,
            id=self.id,
            use_as_name=self.use_as_name,
            use_as_description=self.use_as_description,
            columns_layout=self.columns_layout,
            filter_layout=self.filter_layout,
            properties_layout=self.properties_layout,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.columns_layout:
            output["columnsLayout" if camel_case else "columns_layout"] = [
                _data.dump(camel_case) for _data in self.columns_layout
            ]
        if self.filter_layout:
            output["filterLayout" if camel_case else "filter_layout"] = [
                _data.dump(camel_case) for _data in self.filter_layout
            ]
        if self.properties_layout:
            output["propertiesLayout" if camel_case else "properties_layout"] = [
                _data.dump(camel_case) for _data in self.properties_layout
            ]
        if self.view:
            output["view"] = self.view.dump(camel_case)
        return output


class SearchConfigWrite(SearchConfigCore):
    """
    SearchConfig write/requst format.

    Args:
        view: The configuration for one specific view.
        id: A server-generated ID for the object.
        use_as_name: The name of property to use for the name column in the UI.
        use_as_description: The name of property to use for the description column in the UI.
        columns_layout: Array of column configurations per property.
        filter_layout: Array of filter configurations per property.
        properties_layout: Array of property configurations per property.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource.get("id"),
            view=ViewId.load(resource["view"]),
            use_as_name=resource.get("useAsName"),
            use_as_description=resource.get("useAsDescription"),
            columns_layout=(
                [SearchConfigViewProperty.load(item) for item in items]
                if (items := resource.get("columnsLayout"))
                else None
            ),
            filter_layout=(
                [SearchConfigViewProperty.load(item) for item in items]
                if (items := resource.get("filterLayout"))
                else None
            ),
            properties_layout=(
                [SearchConfigViewProperty.load(item) for item in items]
                if (items := resource.get("propertiesLayout"))
                else None
            ),
        )


class SearchConfig(SearchConfigCore):
    """
    Response model for a single Configuration.

    Args:
        view: The configuration for one specific view.
        id: A server-generated ID for the object.
        created_time: The time when the search config was created.
        updated_time: The time when the search config was last updated.
        use_as_name: The name of property to use for the name column in the UI.
        use_as_description: The name of property to use for the description column in the UI.
        columns_layout: Array of column configurations per property.
        filter_layout: Array of filter configurations per property.
        properties_layout: Array of property configurations per property.
    """

    def __init__(
        self,
        view: ViewId,
        id: int,
        created_time: int,
        updated_time: int,
        use_as_name: str | None = None,
        use_as_description: str | None = None,
        columns_layout: list[SearchConfigViewProperty] | None = None,
        filter_layout: list[SearchConfigViewProperty] | None = None,
        properties_layout: list[SearchConfigViewProperty] | None = None,
    ) -> None:
        super().__init__(
            view,
            id,
            use_as_name,
            use_as_description,
            columns_layout,
            filter_layout,
            properties_layout,
        )
        self.created_time = created_time
        self.updated_time = updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            view=ViewId.load(resource["view"]),
            id=resource["id"],
            created_time=resource["createdTime"],
            updated_time=resource["lastUpdatedTime"],
            use_as_name=resource.get("useAsName"),
            use_as_description=resource.get("useAsDescription"),
            columns_layout=(
                [SearchConfigViewProperty.load(item) for item in items]
                if (items := resource.get("columnsLayout"))
                else None
            ),
            filter_layout=(
                [SearchConfigViewProperty.load(item) for item in items]
                if (items := resource.get("filterLayout"))
                else None
            ),
            properties_layout=(
                [SearchConfigViewProperty.load(item) for item in items]
                if (items := resource.get("propertiesLayout"))
                else None
            ),
        )


class SearchConfigWriteList(CogniteResourceList):
    _RESOURCE = SearchConfigWrite


class SearchConfigList(WriteableCogniteResourceList[SearchConfigWrite, SearchConfig]):
    _RESOURCE = SearchConfig

    def as_write(self) -> SearchConfigWriteList:
        return SearchConfigWriteList([searchConfig.as_write() for searchConfig in self])
