from abc import ABC
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata, FileMetadataWrite, FileMetadataWriteList
from cognite.client.data_classes._base import (
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


class _StreamlitCore(WriteableCogniteResource["StreamlitWrite"], ABC):
    def __init__(
        self,
        external_id: str,
        name: str,
        creator: str,
        entrypoint: str,
        description: str | None = None,
        published: bool = False,
        theme: Literal["Light", "Dark"] = "Light",
        thumbnail: str | None = None,
        data_set_id: int | None = None,
        cognite_toolkit_app_hash: str = "MISSING",
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.creator = creator
        self.entrypoint = entrypoint
        self.description = description
        self.published = published
        self.theme = theme
        self.thumbnail = thumbnail
        self.data_set_id = data_set_id
        self.cognite_toolkit_app_hash = cognite_toolkit_app_hash

    def _as_file_args(self) -> dict[str, Any]:
        metadata = {
            "creator": self.creator,
            "description": self.description,
            "name": self.name,
            "published": self.published,
            "theme": self.theme,
            "entrypoint": self.entrypoint,
            "cdf-toolkit-app-hash": self.cognite_toolkit_app_hash,
        }
        if self.thumbnail:
            metadata["thumbnail"] = self.thumbnail
        return {
            "external_id": self.external_id,
            "name": f"{self.name}-source.json",
            "data_set_id": self.data_set_id,
            "directory": "/streamlit-apps/",
            "metadata": metadata,
        }


class StreamlitWrite(_StreamlitCore):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> "StreamlitWrite":
        args = dict(
            external_id=resource["externalId"],
            name=resource["name"],
            creator=resource["creator"],
            entrypoint=resource["entrypoint"],
            description=resource.get("description"),
            thumbnail=resource.get("thumbnail"),
            data_set_id=resource.get("dataSetId"),
        )
        # Trick to avoid specifying defaults twice
        for key in ["published", "theme"]:
            if key in resource:
                args[key] = resource[key]
        if "cogniteToolkitAppHash" in resource:
            args["cognite_toolkit_app_hash"] = resource["cogniteToolkitAppHash"]
        return cls(**args)

    def as_write(self) -> "StreamlitWrite":
        return self

    def as_file(self) -> FileMetadataWrite:
        return FileMetadataWrite(**self._as_file_args())


class Streamlit(_StreamlitCore):
    def __init__(
        self,
        external_id: str,
        name: str,
        creator: str,
        entrypoint: str,
        created_time: int,
        last_updated_time: int,
        description: str | None = None,
        published: bool = False,
        theme: Literal["Light", "Dark"] = "Light",
        thumbnail: str | None = None,
        data_set_id: int | None = None,
        cognite_toolkit_app_hash: str = "MISSING",
    ) -> None:
        super().__init__(
            external_id,
            name,
            creator,
            entrypoint,
            description,
            published,
            theme,
            thumbnail,
            data_set_id,
            cognite_toolkit_app_hash,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> "Streamlit":
        args = dict(
            external_id=resource["externalId"],
            name=resource["name"],
            creator=resource["creator"],
            entrypoint=resource["entrypoint"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            description=resource.get("description"),
            thumbnail=resource.get("thumbnail"),
            data_set_id=resource.get("dataSetId"),
        )
        # Trick to avoid specifying defaults twice
        for key in ["theme", "app_hash"]:
            if key in resource:
                args[key] = resource[key]
        if "cogniteToolkitAppHash" in resource:
            args["cognite_toolkit_app_hash"] = resource["cogniteToolkitAppHash"]
        if "published" in resource:
            if isinstance(resource["published"], str):
                args["published"] = resource["published"].strip().lower() == "true"
            else:
                args["published"] = resource["published"]
        return cls(**args)

    @classmethod
    def from_file(cls, file: FileMetadata) -> "Streamlit":
        dumped = file.dump()
        if "metadata" in dumped:
            dumped.update(dumped.pop("metadata"))
        if "cdf-toolkit-app-hash" in dumped:
            dumped["cogniteToolkitAppHash"] = dumped.pop("cdf-toolkit-app-hash")
        if "entrypoint" not in dumped:
            dumped["entrypoint"] = "MISSING"
        return cls._load(dumped)

    def as_write(self) -> StreamlitWrite:
        return StreamlitWrite(
            external_id=self.external_id,
            name=self.name,
            creator=self.creator,
            entrypoint=self.entrypoint,
            description=self.description,
            published=self.published,
            theme=self.theme,
            thumbnail=self.thumbnail,
            data_set_id=self.data_set_id,
            cognite_toolkit_app_hash=self.cognite_toolkit_app_hash,
        )

    def as_file(self) -> FileMetadata:
        args = self._as_file_args()
        args.update(
            created_time=self.created_time,
            last_updated_time=self.last_updated_time,
        )
        return FileMetadata(**args)


class StreamlitWriteList(CogniteResourceList[StreamlitWrite], ExternalIDTransformerMixin):
    _RESOURCE = StreamlitWrite

    def as_file_list(self) -> FileMetadataWriteList:
        return FileMetadataWriteList([item.as_file() for item in self])


class StreamlitList(WriteableCogniteResourceList[StreamlitWrite, Streamlit], ExternalIDTransformerMixin):
    _RESOURCE = Streamlit

    def as_write(self) -> StreamlitWriteList:
        return StreamlitWriteList([item.as_write() for item in self])

    def as_file_list(self) -> FileMetadataWriteList:
        return FileMetadataWriteList([item.as_file() for item in self])
