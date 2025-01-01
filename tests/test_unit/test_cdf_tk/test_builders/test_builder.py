from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.builders import TransformationBuilder, get_loader
from cognite_toolkit._cdf_tk.exceptions import AmbiguousResourceFileError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.loaders import RawDatabaseLoader

if not Flags.REQUIRE_KIND.is_enabled():

    def test_get_loader_raises_ambiguous_error():
        builder = TransformationBuilder(Path("build"))

        with pytest.raises(AmbiguousResourceFileError) as e:
            builder._get_loader(
                source_path=Path("my_module") / "transformations" / "notification.yaml",
            )
        assert "Ambiguous resource file" in str(e.value)


def test_get_loader() -> None:
    filepath = MagicMock(spec=Path)
    filepath.name = "filelocation.yaml"
    filepath.stem = "filelocation"
    filepath.suffix = ".yaml"
    filepath.read_text.return_value = "dbName: my_database\n"
    _, loader = get_loader(filepath, "raw", force_pattern=True)

    assert loader is RawDatabaseLoader
