from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.builders import get_resource_crud
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_CRUD_LIST,
    GroupAllScopedCRUD,
    GroupIO,
    GroupResourceScopedCRUD,
    ResourceIO,
)
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitNotSupportedWarning, ToolkitWarning
from cognite_toolkit._cdf_tk.tk_warnings.fileread import UnknownResourceTypeWarning


class TestGetCRUD:
    @pytest.mark.parametrize(
        "source_path, resource_folder, expected_loader_cls",
        [
            pytest.param(
                Path(f"some_path/{crud_cls.folder_name}/my.{crud_cls.kind}.yaml"),
                crud_cls.folder_name,
                {
                    GroupResourceScopedCRUD: GroupIO,
                    GroupAllScopedCRUD: GroupIO,
                }.get(crud_cls, crud_cls),
                id=crud_cls.__name__,
            )
            for crud_cls in RESOURCE_CRUD_LIST
        ],
    )
    def test_get_crud_no_warning(
        self, source_path: Path, resource_folder: str, expected_loader_cls: type[ResourceIO]
    ) -> None:
        crud_cls, warning = get_resource_crud(source_path, resource_folder)

        assert warning is None
        assert crud_cls is expected_loader_cls

    @pytest.mark.parametrize(
        "source_path, resource_folder, expected_warning_cls",
        [
            pytest.param(
                Path(f"some_path/unknown_folder/my.{GroupIO.kind}.yaml"),
                "unknown_folder",
                ToolkitNotSupportedWarning,
                id="Unknown folder, known kind",
            ),
            pytest.param(
                Path("some_path/group/my.UnknownKind.yaml"),
                GroupIO.folder_name,
                UnknownResourceTypeWarning,
                id="Known folder, unknown kind",
            ),
        ],
    )
    def test_get_crud_warning(
        self, source_path: Path, resource_folder: str, expected_warning_cls: type[ToolkitWarning]
    ) -> None:
        crud_cls, warning = get_resource_crud(source_path, resource_folder)

        assert crud_cls is None
        assert type(warning) is expected_warning_cls
