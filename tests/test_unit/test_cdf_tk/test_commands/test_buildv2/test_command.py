from pathlib import Path

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters


class TestBuildCommand:
    def test_end_to_end(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        org = tmp_path / "org"
        org.mkdir()

        parameters = BuildParameters(
            organization_dir=org,
            build_dir=tmp_path / "build",
            build_env_name="dev",
        )

        result = cmd.build_folder(parameters)

        assert result
