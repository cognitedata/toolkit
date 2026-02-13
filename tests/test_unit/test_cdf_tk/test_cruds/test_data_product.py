from collections.abc import Hashable
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceReference
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.data_product import DataProductCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import SpaceCRUD


def _make_response(**overrides: object) -> DataProductResponse:
    defaults: dict = {
        "externalId": "dp_001",
        "name": "My Data Product",
        "schemaSpace": "dp_001",
        "isGoverned": False,
        "tags": [],
        "domains": [],
        "createdTime": 1000,
        "lastUpdatedTime": 2000,
    }
    defaults.update(overrides)
    return DataProductResponse.model_validate(defaults)


def _make_crud() -> DataProductCRUD:
    return DataProductCRUD(MagicMock(), None, None)


class TestDataProductDumpResource:
    """Tests for dump_resource comparison logic."""

    def test_unchanged_when_all_fields_match(self) -> None:
        """When local YAML and CDF have identical values, dump_resource should produce an equal dict."""
        crud = _make_crud()
        local = {
            "externalId": "dp_001",
            "name": "My Data Product",
            "isGoverned": True,
            "tags": ["sales"],
        }
        cdf_response = _make_response(isGoverned=True, tags=["sales"])

        cdf_dict = crud.dump_resource(cdf_response, local)
        assert cdf_dict == local

    def test_schema_space_stripped_from_both_sides(self) -> None:
        """schemaSpace is immutable — should be excluded from comparison even when present in local."""
        crud = _make_crud()
        local = {
            "externalId": "dp_001",
            "name": "My Data Product",
            "schemaSpace": "custom-space",
        }
        cdf_response = _make_response(schemaSpace="dp_001")

        cdf_dict = crud.dump_resource(cdf_response, local)
        assert "schemaSpace" not in cdf_dict
        assert "schemaSpace" not in local

    def test_default_fields_stripped_when_not_in_local(self) -> None:
        """Fields with server defaults should not cause false diffs when omitted from local YAML."""
        crud = _make_crud()
        local: dict = {"externalId": "dp_001", "name": "My Data Product"}
        cdf_response = _make_response(isGoverned=False, tags=[], description=None)

        cdf_dict = crud.dump_resource(cdf_response, local)
        assert cdf_dict == local

    def test_real_change_detected(self) -> None:
        """When a real value differs, the diff should be detected."""
        crud = _make_crud()
        local = {
            "externalId": "dp_001",
            "name": "Updated Name",
            "isGoverned": True,
            "tags": ["new-tag"],
        }
        cdf_response = _make_response(isGoverned=False, tags=["old-tag"])

        cdf_dict = crud.dump_resource(cdf_response, local)
        assert cdf_dict != local

    def test_non_default_governed_kept_when_not_in_local(self) -> None:
        """If CDF has isGoverned=True (non-default) but local omits it, the diff should be detected."""
        crud = _make_crud()
        local: dict = {"externalId": "dp_001", "name": "My Data Product"}
        cdf_response = _make_response(isGoverned=True)

        cdf_dict = crud.dump_resource(cdf_response, local)
        assert "isGoverned" in cdf_dict
        assert cdf_dict != local


class TestDataProductDependencies:
    """Tests for get_dependent_items."""

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {"externalId": "dp_001", "name": "DP", "schemaSpace": "my_space"},
                [(SpaceCRUD, SpaceReference(space="my_space"))],
                id="With schemaSpace",
            ),
            pytest.param(
                {"externalId": "dp_001", "name": "DP"},
                [],
                id="Without schemaSpace",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = DataProductCRUD.get_dependent_items(item)
        assert list(actual) == expected


class TestDataProductUpdate:
    """Tests for as_update excluding immutable fields."""

    def test_as_update_excludes_schema_space(self) -> None:
        """schemaSpace must not appear in the update payload."""
        request = DataProductRequest(
            external_id="dp_001",
            name="My Data Product",
            schema_space="my_space",
            is_governed=True,
            tags=["sales"],
        )
        update_item = request.as_update(mode="replace")

        assert "schemaSpace" not in update_item.get("update", {})
        assert update_item["externalId"] == "dp_001"

    def test_as_update_includes_mutable_fields(self) -> None:
        """Mutable fields should be present in the update payload."""
        request = DataProductRequest(
            external_id="dp_001",
            name="My Data Product",
            is_governed=True,
            tags=["sales", "marketing"],
            description="A description",
        )
        update_item = request.as_update(mode="replace")
        update = update_item["update"]

        assert update["name"] == {"set": "My Data Product"}
        assert update["isGoverned"] == {"set": True}
        assert update["tags"] == {"set": ["sales", "marketing"]}
        assert update["description"] == {"set": "A description"}

    def test_as_update_patch_mode_tags_use_add(self) -> None:
        """In patch mode, container fields (tags) should use 'add' semantics."""
        request = DataProductRequest(
            external_id="dp_001",
            name="My Data Product",
            tags=["new-tag"],
        )
        update_item = request.as_update(mode="patch")
        update = update_item["update"]

        assert update["tags"] == {"add": ["new-tag"]}
