import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.integrations import IntegrationsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.integration import (
    AvailableIntegrationAction,
    GeneralIntegrationError,
    IntegrationActionRequest,
    IntegrationCheckinRequest,
    IntegrationConfigRequest,
    IntegrationExtractor,
    IntegrationRequest,
    IntegrationStartupRequest,
    IntegrationTask,
    IntegrationTaskEvent,
)

_INTEGRATION = {
    "externalId": "my.integrations.id",
    "name": "Pump",
    "extractor": {"externalId": "cognite-opcua", "version": "1.2.3"},
    "createdTime": 1730204346000,
    "lastUpdatedTime": 1730204346000,
}


class TestIntegrationsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_create_retrieve_update_list_and_delete(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = IntegrationsAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/integrations")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_INTEGRATION]})
        )
        respx_mock.post(api._make_url("/integrations/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_INTEGRATION]})
        )
        respx_mock.post(api._make_url("/integrations/update")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_INTEGRATION]})
        )
        respx_mock.get(api._make_url("/integrations")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_INTEGRATION]})
        )
        respx_mock.post(api._make_url("/integrations/delete")).mock(return_value=httpx2.Response(status_code=200))

        request = IntegrationRequest(
            external_id="my.integrations.id",
            name="Pump",
            extractor=IntegrationExtractor(external_id="cognite-opcua", version="1.2.3"),
            metadata={"env": "test"},
        )
        created = api.create([request])
        retrieved = api.retrieve([ExternalId(external_id="my.integrations.id")])
        updated = api.update([request])
        listed = api.list(limit=1)
        api.delete([ExternalId(external_id="my.integrations.id")])

        assert {
            "created": [item.dump() for item in created],
            "version": respx_mock.calls[0].request.headers["cdf-version"],
            "create_request": json.loads(respx_mock.calls[0].request.content),
            "retrieved": [item.external_id for item in retrieved],
            "retrieve_request": json.loads(respx_mock.calls[1].request.content),
            "updated": [item.external_id for item in updated],
            "update_request": json.loads(respx_mock.calls[2].request.content),
            "listed": [item.external_id for item in listed],
            "list_limit": dict(respx_mock.calls[3].request.url.params)["limit"],
            "delete_request": json.loads(respx_mock.calls[4].request.content),
        } == {
            "created": [_INTEGRATION],
            "version": "alpha",
            "create_request": {"items": [request.dump()]},
            "retrieved": ["my.integrations.id"],
            "retrieve_request": {"items": [{"externalId": "my.integrations.id"}], "ignoreUnknownIds": False},
            "updated": ["my.integrations.id"],
            "update_request": {
                "items": [
                    {
                        "externalId": "my.integrations.id",
                        "update": {
                            "name": {"set": "Pump"},
                            "documentation": {"setNull": True},
                            "metadata": {"set": {"env": "test"}},
                            "allowedNotSeenMinutes": {"setNull": True},
                        },
                    }
                ]
            },
            "listed": ["my.integrations.id"],
            "list_limit": "1",
            "delete_request": {"items": [{"externalId": "my.integrations.id"}], "ignoreUnknownIds": False},
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_startup_and_checkin(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = IntegrationsAPI(HTTPClient(toolkit_config))
        checkin_response = {
            "externalId": "my.integrations.id",
            "lastConfigRevision": 3,
            "pendingActions": [],
        }
        respx_mock.post(api._make_url("/integrations/startup")).mock(
            return_value=httpx2.Response(status_code=200, json=checkin_response)
        )
        respx_mock.post(api._make_url("/integrations/checkin")).mock(
            return_value=httpx2.Response(status_code=200, json=checkin_response)
        )

        started = api.startup(
            IntegrationStartupRequest(
                external_id="my.integrations.id",
                extractor=IntegrationExtractor(external_id="cognite-opcua", version="1.2.3"),
                tasks=[IntegrationTask(type="continuous", name="pump-sync")],
                available_actions=[AvailableIntegrationAction(name="restart", type="custom")],
            )
        )
        checked_in = api.checkin(
            IntegrationCheckinRequest(
                external_id="my.integrations.id",
                task_events=[IntegrationTaskEvent(type="started", name="pump-sync", timestamp=1730204346000)],
                errors=[
                    GeneralIntegrationError(
                        level="warning",
                        description="Slow response",
                        start_time=1730204346000,
                        task="pump-sync",
                    )
                ],
            )
        )

        assert {
            "started": started.dump(),
            "startup_request": json.loads(respx_mock.calls[0].request.content),
            "checked_in": checked_in.external_id,
            "checkin_request": json.loads(respx_mock.calls[1].request.content),
        } == {
            "started": checkin_response,
            "startup_request": {
                "externalId": "my.integrations.id",
                "extractor": {"externalId": "cognite-opcua", "version": "1.2.3"},
                "tasks": [{"type": "continuous", "name": "pump-sync", "action": False}],
                "availableActions": [{"name": "restart", "type": "custom"}],
            },
            "checked_in": "my.integrations.id",
            "checkin_request": {
                "externalId": "my.integrations.id",
                "taskEvents": [{"type": "started", "name": "pump-sync", "timestamp": 1730204346000}],
                "errors": [
                    {
                        "type": "general",
                        "level": "warning",
                        "description": "Slow response",
                        "startTime": 1730204346000,
                        "task": "pump-sync",
                    }
                ],
            },
        }


class TestIntegrationTasksAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_list_history_and_sync(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = IntegrationsAPI(HTTPClient(toolkit_config))
        history_item = {
            "taskName": "pump-sync",
            "errorCount": 0,
            "warningCount": 1,
            "fatalCount": 0,
            "startTime": 1730204346000,
        }
        respx_mock.get(api._make_url("/integrations/history")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [history_item]})
        )
        respx_mock.get(api._make_url("/integrations/sync")).mock(
            return_value=httpx2.Response(
                status_code=200,
                json={"nextCursor": "cursor-1", "moreData": False, "history": [history_item], "errors": []},
            )
        )

        listed = api.tasks.list(integration_external_id="my.integrations.id", task_name="pump-sync", limit=10)
        synced = api.tasks.sync(
            "my.integrations.id",
            include_errors=True,
            include_task_updates=True,
            limit=10,
        )

        assert {
            "listed": [item.dump() for item in listed],
            "history_params": dict(respx_mock.calls[0].request.url.params),
            "synced": synced.dump(),
            "sync_params": dict(respx_mock.calls[1].request.url.params),
        } == {
            "listed": [history_item],
            "history_params": {"externalId": "my.integrations.id", "taskName": "pump-sync", "limit": "10"},
            "synced": {"nextCursor": "cursor-1", "moreData": False, "history": [history_item], "errors": []},
            "sync_params": {
                "externalId": "my.integrations.id",
                "includeErrors": "true",
                "includeTaskUpdates": "true",
                "limit": "10",
            },
        }


class TestIntegrationActionsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_create_retrieve_list_and_cancel(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = IntegrationsAPI(HTTPClient(toolkit_config))
        action = {
            "externalId": "action-1",
            "actionName": "restart",
            "status": "pending",
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204346000,
        }
        cancelled = {**action, "status": "cancel_pending"}
        respx_mock.post(api._make_url("/integrations/actions")).mock(
            return_value=httpx2.Response(status_code=201, json={"items": [action]})
        )
        respx_mock.post(api._make_url("/integrations/actions/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [action]})
        )
        respx_mock.get(api._make_url("/integrations/actions")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [action]})
        )
        respx_mock.post(api._make_url("/integrations/actions/cancel")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [cancelled]})
        )

        created = api.actions.create(
            "my.integrations.id",
            [IntegrationActionRequest(external_id="action-1", action_name="restart")],
        )
        retrieved = api.actions.retrieve([ExternalId(external_id="action-1")])
        listed = api.actions.list(integration_external_id="my.integrations.id", limit=1)
        cancelled_actions = api.actions.cancel([ExternalId(external_id="action-1")])

        assert {
            "created": [item.dump() for item in created],
            "create_query": dict(respx_mock.calls[0].request.url.params),
            "create_body": json.loads(respx_mock.calls[0].request.content),
            "retrieved": [item.status for item in retrieved],
            "listed": [item.external_id for item in listed],
            "cancelled": [item.status for item in cancelled_actions],
            "cancel_body": json.loads(respx_mock.calls[3].request.content),
        } == {
            "created": [action],
            "create_query": {"externalId": "my.integrations.id"},
            "create_body": {"items": [{"externalId": "action-1", "actionName": "restart"}]},
            "retrieved": ["pending"],
            "listed": ["action-1"],
            "cancelled": ["cancel_pending"],
            "cancel_body": {"items": [{"externalId": "action-1"}], "ignoreUnknownIds": False},
        }


class TestIntegrationConfigurationAndErrorsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_config_revision_and_error_list(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = IntegrationsAPI(HTTPClient(toolkit_config))
        created_config = {
            "externalId": "my.integrations.id",
            "revision": 4,
            "config": "key: value",
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204346000,
        }
        listed_config = {
            "externalId": "my.integrations.id",
            "revision": 4,
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204346000,
        }
        error = {
            "type": "general",
            "level": "warning",
            "description": "Slow response",
            "details": "The source timed out",
            "task": "pump-sync",
            "startTime": 1730204346000,
        }
        respx_mock.post(api._make_url("/integrations/config")).mock(
            return_value=httpx2.Response(status_code=200, json=created_config)
        )
        respx_mock.get(api._make_url("/integrations/config")).mock(
            return_value=httpx2.Response(status_code=200, json=created_config)
        )
        respx_mock.get(api._make_url("/integrations/config/revisions")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [listed_config]})
        )
        respx_mock.get(api._make_url("/integrations/errors")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [error]})
        )

        created = api.configuration.create(
            [IntegrationConfigRequest(external_id="my.integrations.id", config="key: value")]
        )
        retrieved = api.configuration.retrieve([created[0].as_id()])
        listed = api.configuration.list(integration_external_id="my.integrations.id", limit=1)
        errors = api.errors.list(integration_external_id="my.integrations.id", task="pump-sync", limit=5)

        assert {
            "created": [item.dump() for item in created],
            "create_body": json.loads(respx_mock.calls[0].request.content),
            "retrieved": [item.config for item in retrieved],
            "retrieve_params": dict(respx_mock.calls[1].request.url.params),
            "listed": [item.dump() for item in listed],
            "errors": [item.dump() for item in errors],
            "error_params": dict(respx_mock.calls[3].request.url.params),
        } == {
            "created": [created_config],
            "create_body": {"externalId": "my.integrations.id", "config": "key: value"},
            "retrieved": ["key: value"],
            "retrieve_params": {"externalId": "my.integrations.id", "revision": "4"},
            "listed": [listed_config],
            "errors": [error],
            "error_params": {"externalId": "my.integrations.id", "task": "pump-sync", "limit": "5"},
        }
