import pytest

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
        assert response_instance.dump() == data
