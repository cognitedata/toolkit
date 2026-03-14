from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitValidationError


class TestDeployV2Command:
    @pytest.mark.parametrize(
        "create_build_dir, options, expected",
        [
            pytest.param(
                False,
                None,
                ToolkitNotADirectoryError,
                id="build_dir_does_not_exist",
            ),
            pytest.param(
                True,
                DeployOptions(include=["not_a_real_folder", "also_invalid"]),
                ToolkitValidationError,
                id="include_contains_invalid_folders",
            ),
        ],
    )
    def test_validate_user_input_raises(
        self,
        create_build_dir: bool,
        options: DeployOptions | None,
        expected: type[Exception],
        tmp_path: Path,
    ) -> None:
        build_dir = tmp_path / "build"
        if create_build_dir:
            build_dir.mkdir(parents=True, exist_ok=True)

        with pytest.raises(expected):
            DeployV2Command._validate_user_input(build_dir, options)
