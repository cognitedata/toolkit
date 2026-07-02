"""Unit tests for DataProductVersionIO."""

from collections.abc import Hashable
from typing import ClassVar

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RuleSetVersionId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionQuality,
    DataProductVersionRequest,
    DataProductVersionResponse,
    DataProductVersionView,
)
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.data_product import DataProductIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.data_product_version import DataProductVersionIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.datamodel import ViewIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.rulesets import RuleSetVersionIO
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

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
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
                },
                [
                    (DataProductIO, ExternalId(external_id="my-product")),
                    (ViewIO, ViewId(space="my-space", external_id="MyView", version="1")),
                ],
                id="view reference yields ViewIO dependency",
            ),
            pytest.param(
                {
                    "dataProductExternalId": "my-product",
                    "version": "1.0.0",
                    "quality": {"rules": [{"externalId": "my-ruleset", "version": "1.0.0"}]},
                },
                [
                    (DataProductIO, ExternalId(external_id="my-product")),
                    (
                        RuleSetVersionIO,
                        RuleSetVersionId(rule_set_external_id="my-ruleset", version="1.0.0"),
                    ),
                ],
                id="quality rule yields RuleSetVersionIO dependency",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceIO], Hashable]]) -> None:
        actual = list(DataProductVersionIO.get_dependent_items(item))

        assert actual == expected


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
    _v_cdm_3d: ClassVar[dict] = {
        "space": "cdf_cdm",
        "externalId": "Cognite3DModel",
        "version": "v1",
        "instanceSpaces": {"read": [], "write": []},
    }

    def _existing(self, **overrides: str) -> list[DataProductVersionView]:
        return [DataProductVersionView.model_validate({**self._v_cdm_3d, **overrides})]

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
