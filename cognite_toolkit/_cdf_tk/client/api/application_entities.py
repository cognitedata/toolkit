from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, overload
from urllib.parse import quote

from cognite.client._api_client import APIClient, T
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.application_entities import (
    ApplicationEntity,
    ApplicationEntityList,
    ApplicationEntityWrite,
)


class ApplicationEntitiesAPI(APIClient):
    _RESOURCE_PATH = "/storage"

    def _resource_path(self, data_namespace: str, entity_set: str) -> str:
        return f"{self._RESOURCE_PATH}/{quote(data_namespace)}/{quote(entity_set)}"

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
        # Need to reimplement _create_multiple as that method assumes POST, while this method should use PUT
        is_single_item = not isinstance(item, Sequence)
        items = [item] if not isinstance(item, Sequence) else item
        resource_path = self._resource_path(data_namespace, entity_set)
        tasks = [
            (resource_path, task_items) for task_items in self._prepare_item_chunks(items, self._CREATE_LIMIT, None)
        ]
        summary = execute_tasks(
            self._put,
            tasks,
            max_workers=self._config.max_workers,
        )

        def unwrap_element(el: T) -> ApplicationEntityWrite | T:
            if isinstance(el, dict):
                return ApplicationEntityWrite._load(el, cognite_client=self._cognite_client)
            else:
                return el

        def str_format_element(el: T) -> str | dict | T:
            if isinstance(el, ApplicationEntity):
                dumped = el.dump()
                if "external_id" in dumped:
                    return dumped["external_id"]
                return dumped
            return el

        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1]["items"],
            task_list_element_unwrap_fn=unwrap_element,
            str_format_element_fn=str_format_element,
        )
        created_resources = summary.joined_results(lambda res: res.json()["items"])

        if is_single_item:
            return ApplicationEntity._load(created_resources[0], cognite_client=self._cognite_client)
        return ApplicationEntityList._load(created_resources, cognite_client=self._cognite_client)

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
        return self._retrieve_multiple(
            list_cls=ApplicationEntityList,
            resource_cls=ApplicationEntity,
            identifiers=IdentifierSequence.load(external_ids=external_id),
            resource_path=f"{self._resource_path(data_namespace, entity_set)}",
        )

    def delete(self, external_id: str | SequenceNotStr[str], data_namespace: str, entity_set: str) -> None:
        """Delete an ApplicationEntity.

        Args:
            external_id: The external id of the ApplicationEntity.
            data_namespace: The namespace of the data.
            entity_set: The entity set of the data.

        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            resource_path=f"{self._resource_path(data_namespace, entity_set)}",
        )

    def list(
        self,
        data_namespace: str,
        entity_set: str,
        visibility: Literal["public", "private"] | None = None,
        is_owned: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ApplicationEntityList:
        """List ApplicationEntities.

        Args:
            data_namespace: The namespace of the data.
            entity_set: The entity set of the data.
            visibility: The visibility of the ApplicationEntities.
            is_owned: Whether the ApplicationEntities are owned.
            limit: The maximum number of ApplicationEntities to return. Defaults to 100. Set to -1, float('inf') or None
                to return all items.

        Returns:
            ApplicationEntityList

        """
        filter_: dict[str, Any] = {}
        if visibility is not None:
            filter_["visibility"] = visibility
        if is_owned is not None:
            filter_["isOwned"] = is_owned

        return self._list(
            method="POST",
            list_cls=ApplicationEntityList,
            resource_cls=ApplicationEntity,
            resource_path=self._resource_path(data_namespace, entity_set),
            limit=limit,
            filter=filter_,
        )
