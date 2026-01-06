from typing import Any

import pytest

from cognite_toolkit._cdf_tk.client.data_classes.base import RequestResource, ResponseResource
from tests.test_unit.test_cdf_tk.test_client.data import iterate_response_request_data_triple


class TestAPIDataClasses:
    @pytest.mark.parametrize(
        "response_cls,request_cls,data",
        list(iterate_response_request_data_triple()),
    )
    def test_serialization(
        self, response_cls: type[ResponseResource], request_cls: type[RequestResource], data: dict[str, Any]
    ) -> None:
        response_instance = response_cls.model_validate(data)
        request_instance = response_instance.as_request_resource()
        assert isinstance(request_instance, request_cls)
        resource_id = request_instance.as_id()
        try:
            hash(resource_id)
        except TypeError:
            assert False, f"Resource ID {resource_id} is not hashable"
        assert response_instance.dump() == data
