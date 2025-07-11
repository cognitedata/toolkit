import sys
from collections.abc import Sequence
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import GeoLocation, Label
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.files import FileMetadata, FileMetadataList

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ExtendedFileMetadata(FileMetadata):
    """Extended FileMetadata with pending instance ID support.
    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        instance_id (NodeId | None): The Instance ID for the file. (Only applicable for files created in DMS)
        name (str | None): Name of the file.
        source (str | None): The source of the file.
        mime_type (str | None): File type. E.g., text/plain, application/pdf, ...
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        directory (str | None): Directory associated with the file. It must be an absolute, unix-style path.
        asset_ids (Sequence[int] | None): No description.
        data_set_id (int | None): The dataSet ID for the item.
        labels (Sequence[Label] | None): A list of the labels associated with this resource item.
        geo_location (GeoLocation | None): The geographic metadata of the file.
        source_created_time (int | None): The timestamp for when the file was originally created in the source system.
        source_modified_time (int | None): The timestamp for when the file was last modified in the source system.
        security_categories (Sequence[int] | None): The security category IDs required to access this file.
        id (int | None): A server-generated ID for the object.
        uploaded (bool | None): Whether the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
        uploaded_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        pending_instance_id( NodeId | None): The pending instance ID for the file, used in upgrading files from asset centric to data modeling.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        name: str | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        directory: str | None = None,
        asset_ids: Sequence[int] | None = None,
        data_set_id: int | None = None,
        labels: Sequence[Label] | None = None,
        geo_location: GeoLocation | None = None,
        source_created_time: int | None = None,
        source_modified_time: int | None = None,
        security_categories: Sequence[int] | None = None,
        id: int | None = None,
        uploaded: bool | None = None,
        uploaded_time: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        pending_instance_id: NodeId | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            instance_id=instance_id,
            name=name,
            directory=directory,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            data_set_id=data_set_id,
            labels=labels,
            geo_location=geo_location,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            security_categories=security_categories,
            id=id,
            uploaded=uploaded,
            uploaded_time=uploaded_time,
            created_time=created_time,
            last_updated_time=last_updated_time,
            cognite_client=cognite_client,
        )
        self.pending_instance_id = pending_instance_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.pending_instance_id, dict):
            instance.pending_instance_id = NodeId.load(instance.pending_instance_id)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the object to a dictionary"""
        output = super().dump(camel_case=camel_case)
        if self.pending_instance_id is not None:
            output["pendingInstanceId" if camel_case else "pending_instance_id"] = self.pending_instance_id.dump(
                camel_case=camel_case, include_instance_type=False
            )
        return output


class ExtendedFileMetadataList(FileMetadataList):
    _RESOURCE = ExtendedFileMetadata  # type: ignore[assignment]
