"""Unit tests for DataProductVersionIO."""

from typing import ClassVar

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionQuality,
    DataProductVersionRequest,
    DataProductVersionResponse,
    DataProductVersionView,
)
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios._data_product import DataProductIO
from cognite_toolkit._cdf_tk.resource_ios._data_product_version import DataProductVersionIO
from cognite_toolkit._cdf_tk.resource_ios._datamodel import ViewIO
from cognite_toolkit._cdf_tk.yaml_classes import DataProductVersionYAML


class TestDataProductVersionIODependencies:
    def test_view_is_in_class_dependencies(self) -> None:
        """Regression test for CDF-28000: views must deploy before data product versions."""
        assert ViewIO in DataProductVersionIO.dependencies

    def test_get_dependencies_yields_view_references(self) -> None:
        resource = DataProductVersionYAML.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [
                    {
                        "space": "my-space",
                        "externalId": "MyView",
                        "version": "1",
                    }
                ],
            }
        )

        actual = list(DataProductVersionIO.get_dependencies(resource))

        assert actual == [
            (DataProductIO, ExternalId(external_id="my-product")),
            (ViewIO, ViewId(space="my-space", external_id="MyView", version="1")),
        ]


class TestDataProductVersionIODumpResource:
    def test_dump_resource_without_quality_does_not_raise(self) -> None:
        """Regression test for CDF-27689: dump_resource must not crash when the API
        response omits the optional 'quality' field."""
        client = ToolkitClientMock()
        io = DataProductVersionIO(client)

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
        io = DataProductVersionIO(client)

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

    def test_dump_resource_ignores_stored_instance_spaces(self) -> None:
        client = ToolkitClientMock()
        io = DataProductVersionIO(client)
        view = {"space": "cdf_cdm", "externalId": "Cognite3DModel", "version": "v1"}
        resource = DataProductVersionResponse.model_validate(
            {
                "version": "1.0.0",
                "views": [{**view, "instanceSpaces": {"read": ["my-space"], "write": []}}],
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        )

        dumped = io.dump_resource(resource)

        assert dumped["views"] == [view]

    def test_dump_resource_keeps_instance_spaces_declared_locally(self) -> None:
        client = ToolkitClientMock()
        io = DataProductVersionIO(client)
        instance_spaces = {"read": ["my-space"], "write": []}
        view = {"space": "my_space", "externalId": "MyView", "version": "1"}
        resource = DataProductVersionResponse.model_validate(
            {
                "version": "1.0.0",
                "views": [{**view, "instanceSpaces": instance_spaces}],
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        )
        local = {"views": [{**view, "version": 1, "instanceSpaces": instance_spaces}]}

        dumped = io.dump_resource(resource, local)

        assert dumped["views"] == [{**view, "instanceSpaces": instance_spaces}]


class TestDataProductVersionRequest:
    _v_cdm_3d: ClassVar[dict] = {
        "space": "cdf_cdm",
        "externalId": "Cognite3DModel",
        "version": "v1",
    }

    def _existing(self, **overrides: str) -> list[DataProductVersionView]:
        # Versions created before instanceSpaces was removed from the API still return it.
        stored = {**self._v_cdm_3d, "instanceSpaces": {"read": [], "write": []}, **overrides}
        return [DataProductVersionView.model_validate(stored)]

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

        update = request.as_update(mode="replace", cdf_views=self._existing())

        assert update["update"]["views"] == {
            "add": [
                {
                    "space": "cdf_cdm",
                    "externalId": "OtherView",
                    "version": "v1",
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

        update = request.as_update(mode="replace", cdf_views=self._existing())

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

    def test_as_update_quality_always_omitted(self) -> None:
        """quality is create-only (not in DataProductVersionPatch) so it must never appear in update payloads."""
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "quality": {"rules": [{"externalId": "my-ruleset", "version": "1.0.0"}]},
            }
        )

        for mode in ("patch", "replace"):
            update = request.as_update(mode=mode)  # type: ignore[arg-type]
            assert "quality" not in update["update"], f"quality must not appear in {mode} update payload"

    def test_as_update_replace_view_removed_locally_is_ignored(self) -> None:
        """View refs are immutable/append-only in the API — a view present in CDF but
        absent locally must not generate a remove operation."""
        request = DataProductVersionRequest.model_validate(
            {
                "dataProductExternalId": "my-product",
                "version": "1.0.0",
                "views": [],
            }
        )

        update = request.as_update(mode="replace", cdf_views=self._existing())

        assert "views" not in update["update"]
