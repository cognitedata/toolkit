from typing import Any, Literal

import pytest

from cognite_toolkit._cdf_tk.client.data_classes.agent import AgentRequest
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.data_classes.base import RequestUpdateable
from tests.test_unit.test_cdf_tk.test_client.data import CDFResource, iterate_cdf_resources


class TestAPIDataClasses:
    @pytest.mark.parametrize("resource", list(iterate_cdf_resources()))
    def test_serialization(self, resource: CDFResource) -> None:
        response_cls = resource.response_cls
        request_cls = resource.request_cls
        data = resource.example_data

        response_instance = response_cls.model_validate(data)
        request_instance = response_instance.as_request_resource()
        assert isinstance(request_instance, request_cls)
        resource_id = request_instance.as_id()
        try:
            hash(resource_id)
        except TypeError:
            assert False, f"Resource ID {resource_id} is not hashable"
        if resource.is_dump_equal_to_example:
            assert response_instance.dump() == data

    @pytest.mark.parametrize("resource", list(iterate_cdf_resources()))
    def test_as_update(self, resource: CDFResource) -> None:
        request_instance = resource.request_instance
        if not isinstance(request_instance, RequestUpdateable):
            return
        update_data = request_instance.as_update(mode="patch")
        assert isinstance(update_data, dict)
        assert "update" in update_data


class TestRequestUpdateable:
    """We use the AssetRequest class as a representative example of RequestUpdateable."""

    @pytest.mark.parametrize(
        "request_instance, mode, expected_update",
        [
            pytest.param(
                AssetRequest(externalId="asset_1", name="Asset 1"),
                "patch",
                {"externalId": "asset_1", "update": {"name": {"set": "Asset 1"}}},
                id="Patch with only required field",
            ),
            pytest.param(
                AssetRequest(externalId="asset_1", name="Asset 1"),
                "replace",
                {
                    "externalId": "asset_1",
                    "update": {
                        "dataSetId": {"setNull": True},
                        "description": {"setNull": True},
                        "geoLocation": {"setNull": True},
                        "labels": {"set": []},
                        "metadata": {"set": {}},
                        "name": {"set": "Asset 1"},
                        "source": {"setNull": True},
                    },
                },
                id="Replace with only required field",
            ),
            pytest.param(
                AssetRequest(
                    externalId="asset_1", name="Asset 1", metadata={"key": "value"}, labels=[{"externalId": "label_1"}]
                ),
                "patch",
                {
                    "externalId": "asset_1",
                    "update": {
                        "name": {"set": "Asset 1"},
                        "metadata": {"add": {"key": "value"}},
                        "labels": {"add": [{"externalId": "label_1"}]},
                    },
                },
                id="Patch with container fields",
            ),
            pytest.param(
                AssetRequest(
                    externalId="asset_1", name="Asset 1", metadata={"key": "value"}, labels=[{"externalId": "label_1"}]
                ),
                "replace",
                {
                    "externalId": "asset_1",
                    "update": {
                        "dataSetId": {"setNull": True},
                        "description": {"setNull": True},
                        "geoLocation": {"setNull": True},
                        "labels": {"set": [{"externalId": "label_1"}]},
                        "metadata": {"set": {"key": "value"}},
                        "name": {"set": "Asset 1"},
                        "source": {"setNull": True},
                    },
                },
                id="Replace with container fields",
            ),
        ],
    )
    def test_as_update(
        self, request_instance: RequestUpdateable, mode: Literal["patch", "replace"], expected_update: dict[str, Any]
    ) -> None:
        assert request_instance.as_update(mode=mode) == expected_update


class TestAgentRequest:
    def test_allow_unknown_tool(self) -> None:
        data = {
            "externalId": "agent_1",
            "name": "Agent 1",
            "tools": [
                {
                    "type": "unknown_tool",
                    "name": "Custom Tool",
                    "description": "A tool that is not yet recognized",
                }
            ],
        }
        agent_request = AgentRequest.model_validate(data)
        assert agent_request.dump() == data
