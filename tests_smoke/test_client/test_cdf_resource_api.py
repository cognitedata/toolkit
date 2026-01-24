from collections.abc import Iterable
from typing import Any, get_args, get_origin

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestResource
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from tests_smoke.exceptions import EndpointAssertionError


def crud_cdf_resource_apis() -> Iterable[tuple]:
    subclasses = get_concrete_subclasses(CDFResourceAPI)  # type: ignore[type-abstract]
    for subclass in subclasses:
        if not (hasattr(subclass, "create") and hasattr(subclass, "delete")):
            # Need to be manually tested.
            continue
        yield pytest.param(subclass, id=subclass.__name__)


def get_example_minimum_requests(request_cls: type[RequestResource]) -> dict[str, Any]:
    """Return an example with the only required and identifier fields for the given resource class."""
    requests: dict[type[RequestResource], dict[str, Any]] = {
        AssetRequest: {"name": "smoke-test-asset", "externalId": "smoke-test-asset"},
    }
    try:
        return requests[request_cls]
    except KeyError:
        raise NotImplementedError(f"No example request defined for {request_cls.__name__}")


class TestCDFResourceAPI:
    @pytest.mark.parametrize("api_cls", crud_cdf_resource_apis())
    def test_crud_list(self, api_cls: type[CDFResourceAPI], toolkit_client: ToolkitClient) -> None:
        # Set up
        cdf_resource_base = next((base for base in api_cls.__orig_bases__ if get_origin(base) is CDFResourceAPI), None)  # type: ignore[attr-defined]
        assert cdf_resource_base is not None, "Error in test. Could not find CDFResourceAPI in __orig_bases__"
        request_cls: type[RequestResource]
        _, request_cls, __ = get_args(cdf_resource_base)

        example_data = get_example_minimum_requests(request_cls)

        request = request_cls.model_validate(example_data)
        id = request.as_id()

        # We now that all subclasses only need http_client as argument, even though
        # CDFResourceAPI also require endpoint map (and disable gzip).
        api = api_cls(toolkit_client.http_client)  # type: ignore[call-arg]
        assert hasattr(api, "create") and hasattr(api, "delete"), (
            "API does not support create and delete methods. Cannot run CRUD test."
        )
        methods = api._method_endpoint_map
        try:
            create_endpoint = methods["create"]
            created = api.create([request])
            if len(created) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created item, got {len(created)}")
            created_item = created[0]
            if created_item.as_request_resource().as_id() != id:
                raise EndpointAssertionError(create_endpoint.path, "Created item's ID does not match the requested ID.")
            if hasattr(api, "update"):
                updated_endpoint = methods["update"]
                updated = api.update([request])
                if len(updated) != 1:
                    raise EndpointAssertionError(updated_endpoint.path, f"Expected 1 updated item, got {len(updated)}")
            if hasattr(api, "retrieve"):
                retrieve_endpoint = methods["retrieve"]
                retrieved = api.retrieve([id])
                if len(retrieved) != 1:
                    raise EndpointAssertionError(
                        retrieve_endpoint.path, f"Expected 1 retrieved item, got {len(retrieved)}"
                    )
                retrieved_item = retrieved[0]
                if retrieved_item.as_request_resource().as_id() != id:
                    raise EndpointAssertionError(
                        retrieve_endpoint.path, "Retrieved item's ID does not match the requested ID."
                    )
            if hasattr(api, "list"):
                list_endpoint = methods["list"]
                listed_items = list(api.list(limit=1))
                if len(listed_items) == 0:
                    raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed item, got 0")
        finally:
            api.delete([id])
