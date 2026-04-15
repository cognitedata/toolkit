"""Unit tests for DataProductVersionIO."""

from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionQuality,
    DataProductVersionResponse,
)
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.data_product_version import DataProductVersionIO


class TestDataProductVersionIODumpResource:
    def test_dump_resource_without_quality_does_not_raise(self) -> None:
        """Regression test for CDF-27689: dump_resource must not crash when the API
        response omits the optional 'quality' field."""
        client = ToolkitClientMock()
        io = DataProductVersionIO(client, None, None)

        resource = DataProductVersionResponse(
            data_product_external_id="my-product",
            version="1.0.0",
            created_time=0,
            last_updated_time=0,
            quality=None,
        )

        dumped = io.dump_resource(resource)

        assert "quality" not in dumped

    def test_dump_resource_with_quality_preserved_when_in_local(self) -> None:
        """When quality is present in the local file it must not be stripped from the dump."""
        client = ToolkitClientMock()
        io = DataProductVersionIO(client, None, None)

        resource = DataProductVersionResponse(
            data_product_external_id="my-product",
            version="1.0.0",
            created_time=0,
            last_updated_time=0,
            quality=DataProductVersionQuality(rules=[]),
        )

        local = {
            "dataProductExternalId": "my-product",
            "version": "1.0.0",
            "quality": None,
        }

        dumped = io.dump_resource(resource, local)

        assert "quality" in dumped
