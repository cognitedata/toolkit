from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.builders import get_crud
from cognite_toolkit._cdf_tk.cruds import FileCRUD, RawDatabaseCRUD, RawTableCRUD, ResourceCRUD


class TestGetCRUD:
    @pytest.mark.parametrize(
        "source_path, resource_folder, expected_loader_cls",
        [
            pytest.param(Path("SHOP_model_borgund.File.yaml"), "files", FileCRUD, id="file crud"),
            pytest.param(Path("some_path/raw/my.Database.yaml"), "raw", RawDatabaseCRUD, id="raw database crud"),
            pytest.param(Path("some_path/raw/my.Table.yaml"), "raw", RawTableCRUD, id="raw table crud"),
        ],
    )
    def test_get_crud_no_warning(
        self, source_path: Path, resource_folder: str, expected_loader_cls: type[ResourceCRUD]
    ) -> None:
        crud_cls, warning = get_crud(source_path, resource_folder)

        assert warning is None
        assert crud_cls is expected_loader_cls
