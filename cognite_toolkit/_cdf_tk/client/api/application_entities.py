from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, overload
from urllib.parse import quote

from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.config import ClientConfig
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.application_entities import (
    ApplicationEntity,
    ApplicationEntityList,
    ApplicationEntityWrite,
)


class ApplicationEntitiesAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_version = None
        self._RESOURCE_PATH = f"/apps/v1/projects/{self._cognite_client.config.project}/storage/"

    def _base_url(self, data_namespace: str, entity_set: str) -> str:
        return f"{self._RESOURCE_PATH}{quote(data_namespace)}/{quote(entity_set)}/"

    @overload
    def create(self, item: ApplicationEntityWrite, data_namespace: str, entity_set: str) -> ApplicationEntity: ...

    @overload
    def create(
        self, item: Sequence[ApplicationEntityWrite], data_namespace: str, entity_set: str
    ) -> ApplicationEntityList: ...

    def create(
        self, item: ApplicationEntityWrite | Sequence[ApplicationEntityWrite], data_namespace: str, entity_set: str
    ) -> ApplicationEntity | ApplicationEntityList:
        """Create a new ApplicationEntity.

        Args:
            item: ApplicationEntityWrite or Sequence[ApplicationEntityWrite]
            data_namespace: The namespace of the data.
            entity_set: The entity set of the data.

        Returns:
            ApplicationEntity or ApplicationEntityList

        """
        raise NotImplementedError

    @overload
    def retrieve(self, external_id: str, data_namespace: str, entity_set: str) -> ApplicationEntity: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], data_namespace: str, entity_set: str
    ) -> ApplicationEntityList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], data_namespace: str, entity_set: str
    ) -> ApplicationEntity | ApplicationEntityList:
        """Retrieve an ApplicationEntity.

        Args:
            external_id: The external id of the ApplicationEntity.
            data_namespace: The namespace of the data.
            entity_set: The entity set of the data.

        Returns:
            ApplicationEntity or ApplicationEntityList

        """
        raise NotImplementedError()

    def delete(self, external_id: str | SequenceNotStr[str], data_namespace: str, entity_set: str) -> None:
        """Delete an ApplicationEntity.

        Args:
            external_id: The external id of the ApplicationEntity.
            data_namespace: The namespace of the data.
            entity_set: The entity set of the data.

        """
        raise NotImplementedError()

    def list(
        self,
        visibility: Literal["public", "private"] | None = None,
        is_owned: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ApplicationEntityList:
        """List ApplicationEntities.

        Args:
            visibility: The visibility of the ApplicationEntities.
            is_owned: Whether the ApplicationEntities are owned.
            limit: The maximum number of ApplicationEntities to return. Defaults to 100. Set to -1, float('inf') or None
                to return all items.

        Returns:
            ApplicationEntityList

        """
        raise NotImplementedError()
