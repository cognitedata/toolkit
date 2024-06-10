from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.exceptions import AmbiguousResourceFileError


class TestBuildCommand:
    def test_get_loader_raises_ambiguous_error(self):
        with pytest.raises(AmbiguousResourceFileError) as e:
            BuildCommand()._get_loader(
                "transformations", destination=Path("my_module") / "transformations" / "notification.yaml"
            )
        assert "Ambiguous resource file" in str(e.value)
