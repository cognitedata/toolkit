import string
from typing import get_args

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_LIST, ResourceCRUD
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestResourceCRUD:
    @pytest.mark.parametrize("resource_io_cls", RESOURCE_CRUD_LIST)
    def test_retrieve_non_existing_works(
        self, toolkit_client: ToolkitClient, resource_io_cls: type[ResourceCRUD]
    ) -> None:
        """Test that retrieving a non-existing resource does not raise an error."""
        # Get the identifier class from the generic parameters of the resource_io class
        base_cls = next((base for base in resource_io_cls.__orig_bases__), None)  # type: ignore[attr-defined]
        assert base_cls is not None, f"{resource_io_cls} does not have __orig_bases__"
        classes = get_args(base_cls)
        assert len(classes) == 3, f"{resource_io_cls} should have 3 generic parameters, but has {len(classes)}"
        identifier_cls: type[Identifier] = classes[0]

        non_existing_id = FakeCogniteResourceGenerator(
            seed=37, sample_from_string=string.ascii_letters, min_string_length=3, max_string_length=20
        ).create_instance(identifier_cls)

        resource_io = resource_io_cls.create_loader(toolkit_client)

        try:
            retrieved = resource_io.retrieve(
                [non_existing_id],
            )
        except Exception as e:
            raise AssertionError(
                f"Retrieving non-existing resource id in {resource_io.display_name} raised an error: {e}"
            )
        if len(retrieved) != 0:
            raise AssertionError(
                f"Expected to retrieve 0 resources, but retrieved {len(retrieved)} in {resource_io.display_name}"
            )
