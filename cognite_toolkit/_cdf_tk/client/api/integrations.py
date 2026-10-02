"""Integrations API.

Based on the alpha API specification:

- https://api-docs.cognite.com/20230101-alpha/tag/Integrations
- https://api-docs.cognite.com/20230101-alpha/tag/Integration-Tasks
- https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions
- https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration
- https://api-docs.cognite.com/20230101-alpha/tag/Integration-Errors
"""

from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, IntegrationConfigId
from cognite_toolkit._cdf_tk.client.resource_classes.integration import (
    IntegrationActionRequest,
    IntegrationActionResponse,
    IntegrationCheckinRequest,
    IntegrationCheckinResponse,
    IntegrationConfigListResponse,
    IntegrationConfigRequest,
    IntegrationConfigResponse,
    IntegrationErrorResponse,
    IntegrationRequest,
    IntegrationResponse,
    IntegrationStartupRequest,
    IntegrationSyncResponse,
    IntegrationTaskHistory,
)

_API_VERSION = "alpha"
_CREATE_LIMIT = 20
_ITEM_LIMIT = 100


class IntegrationTasksAPI(CDFResourceAPI[IntegrationTaskHistory]):
    """Task run history for integrations."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/integrations/history", item_limit=_ITEM_LIMIT),
            },
            api_version=_API_VERSION,
        )
        self._sync_endpoint = Endpoint(method="GET", path="/integrations/sync", item_limit=_ITEM_LIMIT)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[IntegrationTaskHistory]:
        return PagedResponse[IntegrationTaskHistory].model_validate_json(response.body)

    def paginate(
        self,
        limit: int = _ITEM_LIMIT,
        cursor: str | None = None,
        integration_external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool | None = None,
    ) -> PagedResponse[IntegrationTaskHistory]:
        """Fetch one page of task history.

        Args:
            limit: Maximum number of history entries to return. The server caps this at 100.
            cursor: Cursor for the next page.
            integration_external_id: Return history for one integration.
            task_name: Return history for one task. Requires an integration.
            last_per_task: Return only the latest run of each task.
        Returns:
            One page of task history.
        """
        return self._paginate(
            limit=limit,
            cursor=cursor,
            params={
                "externalId": integration_external_id,
                "taskName": task_name,
                "lastPerTask": last_per_task,
            },
        )

    def iterate(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool | None = None,
    ) -> Iterable[list[IntegrationTaskHistory]]:
        """Iterate over task history.

        Args:
            limit: Maximum number of entries to return in total. None returns all entries.
            integration_external_id: Return history for one integration.
            task_name: Return history for one task. Requires an integration.
            last_per_task: Return only the latest run of each task.
        Returns:
            Pages of task history.
        """
        return self._iterate(
            limit=limit,
            params={
                "externalId": integration_external_id,
                "taskName": task_name,
                "lastPerTask": last_per_task,
            },
        )

    def list(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool | None = None,
    ) -> list[IntegrationTaskHistory]:
        """List task history.

        Args:
            limit: Maximum number of entries to return. None returns all entries.
            integration_external_id: Return history for one integration.
            task_name: Return history for one task. Requires an integration.
            last_per_task: Return only the latest run of each task.
        Returns:
            Task history entries.
        """
        return self._list(
            limit=limit,
            params={
                "externalId": integration_external_id,
                "taskName": task_name,
                "lastPerTask": last_per_task,
            },
        )

    def sync(
        self,
        integration_external_id: str,
        task_name: str | None = None,
        include_errors: bool | None = None,
        include_task_updates: bool | None = None,
        start_time: int | None = None,
        limit: int = _ITEM_LIMIT,
        cursor: str | None = None,
    ) -> IntegrationSyncResponse:
        """Fetch task history and errors changed since the previous cursor.

        Pass the returned cursor on the next call. When ``more_data`` is false, wait before calling again.

        Args:
            integration_external_id: Integration to sync.
            task_name: Limit the sync to one task.
            include_errors: Include errors in the response.
            include_task_updates: Include task history in the response.
            start_time: Oldest timestamp to include, in milliseconds since epoch.
            limit: Maximum number of updates to return. The server caps this at 100.
            cursor: Cursor from the previous sync response.
        Returns:
            The sync page, including the cursor for the next call.
        """
        if not (0 < limit <= self._sync_endpoint.item_limit):
            raise ValueError(f"Limit must be between 1 and {self._sync_endpoint.item_limit}, got {limit}.")
        params = self._filter_out_none_values(
            {
                "externalId": integration_external_id,
                "taskName": task_name,
                "includeErrors": include_errors,
                "includeTaskUpdates": include_task_updates,
                "startTime": start_time,
                "limit": limit,
                "cursor": cursor,
            }
        )
        request = RequestMessage(
            endpoint_url=self._make_url(self._sync_endpoint.path),
            method=self._sync_endpoint.method,
            parameters=params,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return IntegrationSyncResponse.model_validate_json(response.body)


class IntegrationActionsAPI(CDFResourceAPI[IntegrationActionResponse]):
    """Remote actions requested for an integration."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/integrations/actions", item_limit=_CREATE_LIMIT),
                "retrieve": Endpoint(method="POST", path="/integrations/actions/byids", item_limit=_ITEM_LIMIT),
                "delete": Endpoint(method="POST", path="/integrations/actions/cancel", item_limit=_ITEM_LIMIT),
                "list": Endpoint(method="GET", path="/integrations/actions", item_limit=_ITEM_LIMIT),
            },
            api_version=_API_VERSION,
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[IntegrationActionResponse]:
        return PagedResponse[IntegrationActionResponse].model_validate_json(response.body)

    @staticmethod
    def _assign_integration(items: Sequence[IntegrationActionResponse], integration_external_id: str | None) -> None:
        if integration_external_id is None:
            return
        for item in items:
            item.integration_external_id = integration_external_id

    def create(self, items: Sequence[IntegrationActionRequest]) -> list[IntegrationActionResponse]:
        """Create actions for an integration.

        The extractor is asked to run pending actions the next time it checks in.
        Actions are grouped by ``integration_external_id``, which is sent as a query parameter.

        Args:
            items: Actions to create. At most 20 per request for each integration.
        Returns:
            The created actions.
        """
        results: list[IntegrationActionResponse] = []
        grouped = self._group_items_by_text_field(items, "integration_external_id")
        for (integration_external_id,), group in grouped.items():
            created = self._request_item_response(group, "create", params={"externalId": integration_external_id})
            self._assign_integration(created, integration_external_id)
            results.extend(created)
        return results

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[IntegrationActionResponse]:
        """Retrieve actions by external ID.

        Args:
            items: External IDs to retrieve. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved actions.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def cancel(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[IntegrationActionResponse]:
        """Cancel actions by external ID.

        Only actions that are pending, running, or already cancel-pending can be cancelled.

        Args:
            items: External IDs to cancel. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The cancelled actions.
        """
        return self._request_item_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        limit: int = _ITEM_LIMIT,
        cursor: str | None = None,
        integration_external_id: str | None = None,
        created_after: int | None = None,
        include_completed: bool | None = None,
    ) -> PagedResponse[IntegrationActionResponse]:
        """Fetch one page of actions.

        Args:
            limit: Maximum number of actions to return. The server caps this at 100.
            cursor: Cursor for the next page.
            integration_external_id: Return actions for one integration.
            created_after: Return actions created after this timestamp, in milliseconds since epoch.
            include_completed: Include actions that are no longer pending or running.
        Returns:
            One page of actions.
        """
        page = self._paginate(
            limit=limit,
            cursor=cursor,
            params={
                "externalId": integration_external_id,
                "createdAfter": created_after,
                "includeCompleted": include_completed,
            },
        )
        self._assign_integration(page.items, integration_external_id)
        return page

    def iterate(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
        created_after: int | None = None,
        include_completed: bool | None = None,
    ) -> Iterable[list[IntegrationActionResponse]]:
        """Iterate over actions.

        Args:
            limit: Maximum number of actions to return in total. None returns all actions.
            integration_external_id: Return actions for one integration.
            created_after: Return actions created after this timestamp, in milliseconds since epoch.
            include_completed: Include actions that are no longer pending or running.
        Returns:
            Pages of actions.
        """
        for batch in self._iterate(
            limit=limit,
            params={
                "externalId": integration_external_id,
                "createdAfter": created_after,
                "includeCompleted": include_completed,
            },
        ):
            self._assign_integration(batch, integration_external_id)
            yield batch

    def list(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
        created_after: int | None = None,
        include_completed: bool | None = None,
    ) -> list[IntegrationActionResponse]:
        """List actions.

        Args:
            limit: Maximum number of actions to return. None returns all actions.
            integration_external_id: Return actions for one integration.
            created_after: Return actions created after this timestamp, in milliseconds since epoch.
            include_completed: Include actions that are no longer pending or running.
        Returns:
            Actions.
        """
        items = self._list(
            limit=limit,
            params={
                "externalId": integration_external_id,
                "createdAfter": created_after,
                "includeCompleted": include_completed,
            },
        )
        self._assign_integration(items, integration_external_id)
        return items


class IntegrationConfigurationAPI(CDFResourceAPI[IntegrationConfigListResponse]):
    """Configuration revisions for an integration."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/integrations/config", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/integrations/config", item_limit=1),
                "list": Endpoint(method="GET", path="/integrations/config/revisions", item_limit=_ITEM_LIMIT),
            },
            api_version=_API_VERSION,
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[IntegrationConfigListResponse]:
        return PagedResponse[IntegrationConfigListResponse].model_validate_json(response.body)

    def create(self, items: Sequence[IntegrationConfigRequest]) -> list[IntegrationConfigResponse]:
        """Create configuration revisions.

        Each revision is created with its own request. The extractor is notified on its next check-in.

        Args:
            items: Configuration revisions to create.
        Returns:
            The created revisions, including their revision numbers and config bodies.
        """
        endpoint = self._method_endpoint_map["create"]
        results: list[IntegrationConfigResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=item.dump(),
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            results.append(IntegrationConfigResponse.model_validate_json(response.body))
        return results

    def retrieve(
        self,
        items: Sequence[IntegrationConfigId],
        active_at_time: int | None = None,
        ignore_unknown_ids: bool = False,
    ) -> list[IntegrationConfigResponse]:
        """Retrieve configuration revisions.

        Args:
            items: Integration external IDs and optional revision numbers.
            active_at_time: Return the revision that was active at this timestamp, in milliseconds since epoch.
            ignore_unknown_ids: Skip revisions that are not found.
        Returns:
            The retrieved revisions, including their config bodies.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        results: list[IntegrationConfigResponse] = []
        for item in items:
            parameters = item.dump()
            if active_at_time is not None:
                parameters["activeAtTime"] = active_at_time
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                parameters=parameters,
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                results.append(IntegrationConfigResponse.model_validate_json(response.body))
            else:
                try:
                    response.get_success_or_raise(request)
                except ToolkitAPIError as e:
                    if ignore_unknown_ids and e.code == 404:
                        continue
                    raise
        return results

    def paginate(
        self,
        limit: int = _ITEM_LIMIT,
        cursor: str | None = None,
        integration_external_id: str | None = None,
    ) -> PagedResponse[IntegrationConfigListResponse]:
        """Fetch one page of configuration revision metadata.

        Listed revisions omit the config body.

        Args:
            limit: Maximum number of revisions to return. The server caps this at 100.
            cursor: Cursor for the next page.
            integration_external_id: Return revisions for one integration.
        Returns:
            One page of configuration revisions.
        """
        return self._paginate(limit=limit, cursor=cursor, params={"externalId": integration_external_id})

    def iterate(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
    ) -> Iterable[list[IntegrationConfigListResponse]]:
        """Iterate over configuration revision metadata.

        Listed revisions omit the config body.

        Args:
            limit: Maximum number of revisions to return in total. None returns all revisions.
            integration_external_id: Return revisions for one integration.
        Returns:
            Pages of configuration revisions.
        """
        return self._iterate(limit=limit, params={"externalId": integration_external_id})

    def list(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
    ) -> list[IntegrationConfigListResponse]:
        """List configuration revision metadata.

        Listed revisions omit the config body.

        Args:
            limit: Maximum number of revisions to return. None returns all revisions.
            integration_external_id: Return revisions for one integration.
        Returns:
            Configuration revisions.
        """
        return self._list(limit=limit, params={"externalId": integration_external_id})


class IntegrationErrorsAPI(CDFResourceAPI[IntegrationErrorResponse]):
    """Errors reported by integrations."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/integrations/errors", item_limit=_ITEM_LIMIT),
            },
            api_version=_API_VERSION,
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[IntegrationErrorResponse]:
        return PagedResponse[IntegrationErrorResponse].model_validate_json(response.body)

    def paginate(
        self,
        limit: int = _ITEM_LIMIT,
        cursor: str | None = None,
        integration_external_id: str | None = None,
        task: str | None = None,
        min_start_time: int | None = None,
        max_end_time: int | None = None,
    ) -> PagedResponse[IntegrationErrorResponse]:
        """Fetch one page of errors.

        Args:
            limit: Maximum number of errors to return. The server caps this at 100.
            cursor: Cursor for the next page.
            integration_external_id: Return errors for one integration.
            task: Return errors for one task. Requires an integration.
            min_start_time: Return errors that started at or after this timestamp, in milliseconds since epoch.
            max_end_time: Return errors that ended at or before this timestamp, in milliseconds since epoch.
        Returns:
            One page of errors.
        """
        return self._paginate(
            limit=limit,
            cursor=cursor,
            params=self._error_params(integration_external_id, task, min_start_time, max_end_time),
        )

    def iterate(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
        task: str | None = None,
        min_start_time: int | None = None,
        max_end_time: int | None = None,
    ) -> Iterable[list[IntegrationErrorResponse]]:
        """Iterate over errors.

        Args:
            limit: Maximum number of errors to return in total. None returns all errors.
            integration_external_id: Return errors for one integration.
            task: Return errors for one task. Requires an integration.
            min_start_time: Return errors that started at or after this timestamp, in milliseconds since epoch.
            max_end_time: Return errors that ended at or before this timestamp, in milliseconds since epoch.
        Returns:
            Pages of errors.
        """
        return self._iterate(
            limit=limit,
            params=self._error_params(integration_external_id, task, min_start_time, max_end_time),
        )

    def list(
        self,
        limit: int | None = None,
        integration_external_id: str | None = None,
        task: str | None = None,
        min_start_time: int | None = None,
        max_end_time: int | None = None,
    ) -> list[IntegrationErrorResponse]:
        """List errors.

        Args:
            limit: Maximum number of errors to return. None returns all errors.
            integration_external_id: Return errors for one integration.
            task: Return errors for one task. Requires an integration.
            min_start_time: Return errors that started at or after this timestamp, in milliseconds since epoch.
            max_end_time: Return errors that ended at or before this timestamp, in milliseconds since epoch.
        Returns:
            Errors.
        """
        return self._list(
            limit=limit,
            params=self._error_params(integration_external_id, task, min_start_time, max_end_time),
        )

    @staticmethod
    def _error_params(
        integration_external_id: str | None,
        task: str | None,
        min_start_time: int | None,
        max_end_time: int | None,
    ) -> dict[str, Any]:
        return {
            "externalId": integration_external_id,
            "task": task,
            "minStartTime": min_start_time,
            "maxEndTime": max_end_time,
        }


class IntegrationsAPI(CDFResourceAPI[IntegrationResponse]):
    """Integrations, plus their tasks, actions, configuration, and errors."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/integrations", item_limit=_CREATE_LIMIT),
                "retrieve": Endpoint(method="POST", path="/integrations/byids", item_limit=_ITEM_LIMIT),
                "update": Endpoint(method="POST", path="/integrations/update", item_limit=_CREATE_LIMIT),
                "delete": Endpoint(method="POST", path="/integrations/delete", item_limit=_ITEM_LIMIT),
                "list": Endpoint(method="GET", path="/integrations", item_limit=_ITEM_LIMIT),
            },
            api_version=_API_VERSION,
        )
        self.tasks = IntegrationTasksAPI(http_client)
        self.actions = IntegrationActionsAPI(http_client)
        self.configuration = IntegrationConfigurationAPI(http_client)
        self.errors = IntegrationErrorsAPI(http_client)
        self._startup_endpoint = Endpoint(method="POST", path="/integrations/startup", item_limit=1)
        self._checkin_endpoint = Endpoint(method="POST", path="/integrations/checkin", item_limit=1)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[IntegrationResponse]:
        return PagedResponse[IntegrationResponse].model_validate_json(response.body)

    def create(self, items: Sequence[IntegrationRequest]) -> list[IntegrationResponse]:
        """Create integrations.

        Args:
            items: Integrations to create. At most 20 per request.
        Returns:
            The created integrations.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[IntegrationResponse]:
        """Retrieve integrations by external ID.

        Args:
            items: External IDs to retrieve. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved integrations.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[IntegrationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[IntegrationResponse]:
        """Update integrations.

        Args:
            items: Integrations to update. At most 20 per request.
            mode: ``replace`` sets every provided field. ``patch`` sets only fields that were assigned.
        Returns:
            The updated integrations.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete integrations by external ID.

        Deleting an integration removes its task and error history from monitoring.

        Args:
            items: External IDs to delete. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(self, limit: int = _ITEM_LIMIT, cursor: str | None = None) -> PagedResponse[IntegrationResponse]:
        """Fetch one page of integrations.

        Args:
            limit: Maximum number of integrations to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of integrations.
        """
        return self._paginate(limit=limit, cursor=cursor)

    def iterate(self, limit: int | None = None) -> Iterable[list[IntegrationResponse]]:
        """Iterate over integrations.

        Args:
            limit: Maximum number of integrations to return in total. None returns all integrations.
        Returns:
            Pages of integrations.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[IntegrationResponse]:
        """List integrations.

        Args:
            limit: Maximum number of integrations to return. None returns all integrations.
        Returns:
            Integrations.
        """
        return self._list(limit=limit)

    def startup(self, item: IntegrationStartupRequest) -> IntegrationCheckinResponse:
        """Report extractor info and mark the integration as started.

        This closes any tasks that are currently running.

        Args:
            item: Extractor, task, and available-action description.
        Returns:
            The latest config revision and any actions waiting for the extractor.
        """
        return self._post_single(self._startup_endpoint, item.dump())

    def checkin(self, item: IntegrationCheckinRequest) -> IntegrationCheckinResponse:
        """Report that an extractor is still alive, along with events since the previous check-in.

        Args:
            item: Task events, errors, and action updates since the previous check-in.
        Returns:
            The latest config revision and any actions waiting for the extractor.
        """
        return self._post_single(self._checkin_endpoint, item.dump())

    def _post_single(self, endpoint: Endpoint, body: dict[str, Any]) -> IntegrationCheckinResponse:
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
            body_content=body,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return IntegrationCheckinResponse.model_validate_json(response.body)
