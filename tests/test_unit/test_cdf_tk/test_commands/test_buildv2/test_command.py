from pathlib import Path

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters


class TestBuildCommand:
    def test_end_to_end(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        parameters = BuildParameters()

        result = cmd.build_folder(parameters)

        assert result
