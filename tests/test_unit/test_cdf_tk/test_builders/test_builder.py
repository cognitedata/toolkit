from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.builders import get_loader
from cognite_toolkit._cdf_tk.cruds import FileCRUD, RawDatabaseCRUD, RawTableCRUD, ResourceCRUD


@pytest.mark.parametrize(
    "content, expected_loader_cls",
    [
        ("dbName: my_database\n", RawDatabaseCRUD),
        ("dbName: my_database\ntableName: my_table\n", RawTableCRUD),
    ],
)
def test_get_loader_raw_loaders(content: str, expected_loader_cls: type[ResourceCRUD]) -> None:
    filepath = MagicMock(spec=Path)
    filepath.name = "filelocation.yaml"
    filepath.stem = "filelocation"
    filepath.suffix = ".yaml"
    filepath.read_text.return_value = content

    loader, warn = get_loader(filepath, "raw", force_pattern=True)

    assert warn is None
    assert loader is expected_loader_cls


def test_get_loader_file() -> None:
    loader_cls, warning = get_loader(Path("SHOP_model_borgund.File.yaml"), "files", force_pattern=True)

    assert warning is None
    assert loader_cls is FileCRUD
