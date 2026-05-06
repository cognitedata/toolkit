"""Unit tests for DataProductVersionIO."""

from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionQuality,
    DataProductVersionRequest,
    DataProductVersionResponse,
    DataProductVersionView,
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


class TestDataProductVersionRequest:
    _v_cdm_3d: ClassVar[dict[str, Any]] = {
        "space": "cdf_cdm",
        "externalId": "Cognite3DModel",
        "version": "v1",
        "instanceSpaces": {"read": [], "write": []},
    }

    def test_as_update_replace_adds_only_net_new_views(self) -> None:
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [
                    {"space": "cdf_cdm", "externalId": "Cognite3DModel", "version": "v1"},
                    {"space": "cdf_cdm", "externalId": "OtherView", "version": "v1"},
                ],
            }
        )
        existing = [
            DataProductVersionView.model_validate(self._v_cdm_3d),
        ]

        update = request.as_update(mode="replace", cdf_views=existing)

        assert update["update"]["views"] == {
            "add": [
                {
                    "space": "cdf_cdm",
                    "externalId": "OtherView",
                    "version": "v1",
                    "instanceSpaces": {"read": [], "write": []},
                }
            ]
        }

    def test_as_update_replace_removes_views_no_longer_desired(self) -> None:
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [{"space": "cdf_cdm", "externalId": "Cognite3DModel", "version": "v1"}],
            }
        )
        existing = [
            DataProductVersionView.model_validate(self._v_cdm_3d),
            DataProductVersionView.model_validate(
                {
                    "space": "cdf_cdm",
                    "externalId": "RemoveMe",
                    "version": "v1",
                    "instanceSpaces": {"read": [], "write": []},
                }
            ),
        ]

        update = request.as_update(mode="replace", cdf_views=existing)

        assert update["update"]["views"] == {
            "remove": [
                {
                    "space": "cdf_cdm",
                    "externalId": "RemoveMe",
                    "version": "v1",
                    "instanceSpaces": {"read": [], "write": []},
                }
            ]
        }

    def test_as_update_replace_unchanged_views_omits_views(self) -> None:
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [{"space": "cdf_cdm", "externalId": "Cognite3DModel", "version": "v1"}],
            }
        )
        existing = [DataProductVersionView.model_validate(self._v_cdm_3d)]

        update = request.as_update(mode="replace", cdf_views=existing)

        assert "views" not in update["update"]

    def test_as_update_replace_empty_cdf_is_all_add(self) -> None:
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [{"space": "cdf_cdm", "externalId": "Cognite3DModel", "version": "v1"}],
            }
        )

        update = request.as_update(mode="replace", cdf_views=[])

        assert update["update"]["views"] == {"add": [self._v_cdm_3d]}

    def test_as_update_replace_empty_desired_removes_all(self) -> None:
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [],
            }
        )
        existing = [DataProductVersionView.model_validate(self._v_cdm_3d)]

        update = request.as_update(mode="replace", cdf_views=existing)

        assert update["update"]["views"] == {"remove": [self._v_cdm_3d]}
