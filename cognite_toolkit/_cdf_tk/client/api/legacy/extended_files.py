from collections.abc import Sequence
from typing import overload

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api.files import FilesAPI
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._auxiliary import split_into_chunks, unpack_items_in_payload
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.extended_filemetadata import (
    ExtendedFileMetadata,
    ExtendedFileMetadataList,
)
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId


class ExtendedFileMetadataAPI(FilesAPI):
    """Extended FileMetadata to include pending ID methods."""

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._PENDING_IDS_LIMIT = 1000

    @overload
    def set_pending_ids(
        self, instance_id: NodeId | tuple[str, str], id: int | None = None, external_id: str | None = None
    ) -> ExtendedFileMetadata: ...

    @overload
    def set_pending_ids(self, instance_id: Sequence[PendingInstanceId]) -> ExtendedFileMetadataList: ...

    def set_pending_ids(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[PendingInstanceId],
        id: int | None = None,
        external_id: str | None = None,
    ) -> ExtendedFileMetadata | ExtendedFileMetadataList:
        """Set a pending identifier for one or more filemetadata.

        Args:
            instance_id: The pending instance ID to set.
            id: The ID of the files.
            external_id: The external ID of the files.

        Returns:
            ExtendedFileMetadata: If a single instance ID is provided, returns the updated ExtendedFileMetadata object.
        """
        if isinstance(instance_id, NodeId) or (
            isinstance(instance_id, tuple)
            and len(instance_id) == 2
            and isinstance(instance_id[0], str)
            and isinstance(instance_id[1], str)
        ):
            return self._set_pending_ids([PendingInstanceId(NodeId.load(instance_id), id=id, external_id=external_id)])[  # type: ignore[return-value]
                0
            ]
        elif isinstance(instance_id, Sequence) and all(isinstance(item, PendingInstanceId) for item in instance_id):
            return self._set_pending_ids(instance_id)  # type: ignore[arg-type]
        else:
            raise TypeError(
                "instance_id must be a NodeId, a tuple of (str, str), or a sequence of PendingIdentifier objects."
            )

    def _set_pending_ids(self, identifiers: Sequence[PendingInstanceId]) -> ExtendedFileMetadataList:
        tasks = [
            {
                "url_path": f"{self._RESOURCE_PATH}/set-pending-instance-ids",
                "json": {
                    "items": [identifier.dump(camel_case=True) for identifier in id_chunk],
                },
                "api_subversion": "alpha",
            }
            for id_chunk in split_into_chunks(list(identifiers), self._PENDING_IDS_LIMIT)
        ]
        tasks_summary = execute_tasks(
            self._post,
            tasks,
            max_workers=self._config.max_workers,
            fail_fast=True,
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
        )

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        return ExtendedFileMetadataList._load(retrieved_items, cognite_client=self._cognite_client)

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> ExtendedFileMetadata | None:
        """`Retrieve a single file metadata by id. <https://developer.cognite.com/api#tag/Files/operation/getFileByInternalId>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            instance_id (NodeId | None): Instance ID

        Returns:
            FileMetadata | None: Requested file metadata or None if it does not exist.

        Examples:

            Get file metadata by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.retrieve(id=1)

            Get file metadata by external id:

                >>> res = client.files.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=ExtendedFileMetadataList,
            resource_cls=ExtendedFileMetadata,
            identifiers=identifiers,
            api_subversion="alpha",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtendedFileMetadataList:
        """`Retrieve multiple file metadatas by id. <https://developer.cognite.com/api#tag/Files/operation/byIdsFiles>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            instance_ids (Sequence[NodeId] | None): Instance IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FileMetadataList: The requested file metadatas.

        Examples:

            Get file metadatas by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.files.retrieve_multiple(ids=[1, 2, 3])

            Get file_metadatas by external id:

                >>> res = client.files.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=ExtendedFileMetadataList,
            resource_cls=ExtendedFileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            api_subversion="alpha",
        )

    @overload
    def unlink_instance_ids(
        self,
        id: int | None = None,
        external_id: str | None = None,
    ) -> ExtendedFileMetadata | None: ...

    @overload
    def unlink_instance_ids(
        self,
        id: Sequence[int] | None = None,
        external_id: SequenceNotStr[str] | None = None,
    ) -> ExtendedFileMetadataList: ...

    def unlink_instance_ids(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> ExtendedFileMetadata | ExtendedFileMetadataList | None:
        """Unlink instance IDs from files.

        This allows a CogniteFile node in Data Modeling to be deleted without deleting the underlying file content.

        Args:
            id (int | Sequence[int] | None): The ID(s) of the files.
            external_id (str | SequenceNotStr[str] | None): The external ID(s) of the files.

        Returns:
            ExtendedFileMetadata | ExtendedFileMetadataList | None: The updated file metadata object(s). For single item requests, returns `None` if the file is not found.
        """
        if id is None and external_id is None:
            raise ValueError("At least one of id or external_id must be provided.")
        if isinstance(id, int) and isinstance(external_id, str):
            raise ValueError("Cannot specify both id and external_id as single values. Use one or the other.")
        is_list = isinstance(id, Sequence) or (isinstance(external_id, Sequence) and not isinstance(external_id, str))
        identifiers = IdentifierSequence.load(id, external_id)

        tasks = [
            {
                "url_path": f"{self._RESOURCE_PATH}/unlink-instance-ids",
                "json": {"items": id_chunk},
                "api_subversion": "alpha",
            }
            for id_chunk in split_into_chunks(identifiers.as_dicts(), 1000)
        ]
        tasks_summary = execute_tasks(
            self._post,
            tasks,
            max_workers=self._config.max_workers,
            fail_fast=True,
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
        )

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        result = ExtendedFileMetadataList._load(retrieved_items, cognite_client=self._cognite_client)
        if is_list:
            return result
        if len(result) == 0:
            return None
        if len(result) > 1:
            raise ValueError("Expected a single file, but multiple were returned.")
        return result[0]  # type: ignore[return-value]
