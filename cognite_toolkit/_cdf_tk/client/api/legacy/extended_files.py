from collections.abc import Sequence

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api.files import FilesAPI
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.files import FileMetadata, FileMetadataList
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class ExtendedFileMetadataAPI(FilesAPI):
    """Extended FileMetadata API that uses the alpha API version for retrieve/list
    to include pending instance ID information in responses."""

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> FileMetadata | None:
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            api_subversion="alpha",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            api_subversion="alpha",
        )
