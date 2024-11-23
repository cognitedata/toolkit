from __future__ import annotations

from abc import ABC
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResourceList, WriteableCogniteResourceList
from cognite.client.data_classes.data_modeling import DataModelId, ViewId
from cognite.client.data_classes.data_modeling.core import DataModelingSchemaResource


class _GraphQLDataModelCore(DataModelingSchemaResource["GraphQLDataModelWrite"], ABC):
    def __init__(
        self, space: str, external_id: str, version: str, name: str | None = None, description: str | None = None
    ) -> None:
        super().__init__(space=space, external_id=external_id, name=name, description=description)
        self.version = version

    def as_id(self) -> DataModelId:
        return DataModelId(space=self.space, external_id=self.external_id, version=self.version)


class GraphQLDataModelWrite(_GraphQLDataModelCore):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        name: str | None = None,
        description: str | None = None,
        previous_version: str | None = None,
        dml: str | None = None,
        preserve_dml: bool | None = None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, version=version, name=name, description=description)
        self.previous_version = previous_version
        self.dml = dml
        self.preserve_dml = preserve_dml

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> GraphQLDataModelWrite:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=str(resource["version"]),
            name=resource.get("name"),
            description=resource.get("description"),
            previous_version=resource.get("previousVersion"),
            dml=resource.get("dml"),
            preserve_dml=resource.get("preserveDml"),
        )

    def as_write(self) -> GraphQLDataModelWrite:
        return self


class GraphQLDataModel(_GraphQLDataModelCore):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: str | None,
        name: str | None,
        views: list[ViewId] | None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, version=version, name=name, description=description)
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.views = views

    def as_write(self) -> GraphQLDataModelWrite:
        return GraphQLDataModelWrite(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
            name=self.name,
            description=self.description,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> GraphQLDataModel:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=str(resource["version"]),
            is_global=resource["isGlobal"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            description=resource.get("description"),
            name=resource.get("name"),
            views=[
                ViewId(space=view["space"], external_id=view["externalId"], version=view.get("version"))
                for view in resource.get("views", [])
            ],
        )


class GraphQLDataModelWriteList(CogniteResourceList[GraphQLDataModelWrite]):
    _RESOURCE = GraphQLDataModelWrite

    def as_ids(self) -> list[DataModelId]:
        return [model.as_id() for model in self.data]


class GraphQLDataModelList(WriteableCogniteResourceList[GraphQLDataModelWrite, GraphQLDataModel]):
    _RESOURCE = GraphQLDataModel

    def as_write(self) -> GraphQLDataModelWriteList:
        return GraphQLDataModelWriteList([model.as_write() for model in self.data])

    def as_ids(self) -> list[DataModelId]:
        return [model.as_id() for model in self.data]
