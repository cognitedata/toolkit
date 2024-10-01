from __future__ import annotations

from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.builders import TransformationBuilder
from cognite_toolkit._cdf_tk.exceptions import AmbiguousResourceFileError


def test_get_loader_raises_ambiguous_error():
    builder = TransformationBuilder(Path("build"))

    with pytest.raises(AmbiguousResourceFileError) as e:
        builder._get_loader(
            source_path=Path("my_module") / "transformations" / "notification.yaml",
        )
    assert "Ambiguous resource file" in str(e.value)
