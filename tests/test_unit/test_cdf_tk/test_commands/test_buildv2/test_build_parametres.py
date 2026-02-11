from pathlib import Path
from unittest.mock import MagicMock

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.exceptions import ToolkitError


class TestBuildParameters:
    @pytest.mark.parametrize(
        "paths, parameters, user_args, expected_error",
        [
            pytest.param(
                [],
                BuildParameters(organization_dir=Path("non_existent_org")),
                ["build"],
                "Organization directory 'non_existent_org' not found",
                id="Organization directory does not exist",
            ),
        ],
    )
    def test_validate_build_parameters(
        self, paths: list[Path], parameters: BuildParameters, user_args: list[str], expected_error: str, tmp_path: Path
    ) -> None:
        # Set up the organization directory and config file if needed
        for path in paths:
            if path.suffix:  # If the path has a suffix, it's a file
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
        console = MagicMock(spec=Console)
        with pytest.raises(ToolkitError) as exc_info:
            BuildV2Command.validate_build_parameters(parameters, console, user_args)

        assert expected_error in str(exc_info.value)
