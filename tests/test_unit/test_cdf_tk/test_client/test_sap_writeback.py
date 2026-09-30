import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.sap_writeback import SAPEndpointsAPI, SAPWritebackAPI, SchemaMappingsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WritebackRequestId
from cognite_toolkit._cdf_tk.client.resource_classes.sap_writeback import (
    SAPEndpointConnectionCheck,
    WritebackRequestItemRequest,
    WritebackRequestRequest,
)


class TestSchemaMappingsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_retrieve_lifts_nested_expression(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = SchemaMappingsAPI(HTTPClient(toolkit_config))
        documented_item = {
            "externalId": "my.known.id",
            "mapping": {"expression": '{ "SAPFieldA": input.CDFFieldA }'},
            "input": {"type": "json"},
            "published": True,
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204346000,
        }
        respx_mock.post(api._make_url("/writeback/sap/mappings/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [documented_item]})
        )

        retrieved = api.retrieve([ExternalId(external_id="my.known.id")])

        assert {
            "items": [item.dump() for item in retrieved],
            "request": json.loads(respx_mock.calls[0].request.content),
        } == {
            "items": [
                {
                    "externalId": "my.known.id",
                    "expression": '{ "SAPFieldA": input.CDFFieldA }',
                    "mapping": {"expression": '{ "SAPFieldA": input.CDFFieldA }'},
                    "input": {"type": "json"},
                    "published": True,
                    "createdTime": 1730204346000,
                    "lastUpdatedTime": 1730204346000,
                }
            ],
            "request": {
                "items": [{"externalId": "my.known.id"}],
                "ignoreUnknownIds": False,
            },
        }


class TestSAPEndpointsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_verify_posts_external_id(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = SAPEndpointsAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/writeback/sap/endpoints/verify")).mock(
            return_value=httpx2.Response(status_code=200, json={"status": "success"})
        )

        result = api.verify(ExternalId(external_id="sap_endpoint_001"))

        assert {
            "status": result.status,
            "detail": result.detail,
            "request": json.loads(respx_mock.calls[0].request.content),
        } == {
            "status": "success",
            "detail": None,
            "request": {"externalId": "sap_endpoint_001"},
        }

    def test_verify_unwraps_error_envelope(self) -> None:
        result = SAPEndpointConnectionCheck.model_validate(
            {"error": {"status": "success", "detail": "Connected", "errorMessage": "none"}}
        )

        assert result.dump() == {"status": "success", "detail": "Connected", "errorMessage": "none"}


class TestSAPWritebackAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_create_retrieve_and_list(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = SAPWritebackAPI(HTTPClient(toolkit_config))
        created_item = {
            "requestId": "request-001",
            "status": "pending",
            "request": [{"payload": {"NotificationText": "Test"}}],
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204346000,
        }
        retrieved_item = {
            "requestId": "request-001",
            "status": "done",
            "request": [{"payload": {"NotificationText": "Test"}, "sapObjectId": "10129745"}],
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204347000,
        }
        listed_item = {
            "requestId": "request-001",
            "status": "done",
            "request": [{"sapObjectId": "10129745"}],
            "createdTime": 1730204346000,
            "lastUpdatedTime": 1730204347000,
        }
        respx_mock.post(api._make_url("/writeback/sap/requests")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [created_item]})
        )
        respx_mock.post(api._make_url("/writeback/sap/requests/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [retrieved_item]})
        )
        respx_mock.get(api._make_url("/writeback/sap/requests")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [listed_item]})
        )

        created = api.create(
            [
                WritebackRequestRequest(
                    endpoint_id="sap_endpoint_001",
                    request=[WritebackRequestItemRequest(payload={"NotificationText": "Test"})],
                )
            ]
        )
        retrieved = api.retrieve([WritebackRequestId(request_id="request-001")])
        listed = api.list(limit=1)

        assert {
            "created": [item.dump() for item in created],
            "create_request": json.loads(respx_mock.calls[0].request.content),
            "retrieved": [item.dump() for item in retrieved],
            "retrieve_request": json.loads(respx_mock.calls[1].request.content),
            "listed": [item.dump() for item in listed],
        } == {
            "created": [created_item],
            "create_request": {
                "items": [
                    {
                        "endpointId": "sap_endpoint_001",
                        "request": [{"payload": {"NotificationText": "Test"}}],
                    }
                ]
            },
            "retrieved": [retrieved_item],
            "retrieve_request": {"items": [{"requestId": "request-001"}], "ignoreUnknownIds": False},
            "listed": [listed_item],
        }
