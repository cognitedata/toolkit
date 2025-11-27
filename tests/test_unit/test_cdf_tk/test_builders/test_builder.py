from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.builders import get_resource_crud
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_LIST,
    GroupAllScopedCRUD,
    GroupCRUD,
    GroupResourceScopedCRUD,
    ResourceCRUD,
)


class TestGetCRUD:
    @pytest.mark.parametrize(
        "source_path, resource_folder, expected_loader_cls",
        [
            pytest.param(
                Path(f"some_path/{crud_cls.folder_name}/my.{crud_cls.kind}.yaml"),
                crud_cls.folder_name,
                {
                    GroupResourceScopedCRUD: GroupCRUD,
                    GroupAllScopedCRUD: GroupCRUD,
                }.get(crud_cls, crud_cls),
                id=crud_cls.__name__,
            )
            for crud_cls in RESOURCE_CRUD_LIST
        ],
    )
    def test_get_crud_no_warning(
        self, source_path: Path, resource_folder: str, expected_loader_cls: type[ResourceCRUD]
    ) -> None:
        crud_cls, warning = get_resource_crud(source_path, resource_folder)

        assert warning is None
        assert crud_cls is expected_loader_cls
