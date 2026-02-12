from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import TypeAdapter
from rich.console import Console

from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters, RelativeDirPath
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.exceptions import ToolkitError


class TestBuildParameters:
    @pytest.mark.parametrize(
        "paths, parameters, user_args, expected_error",
        [
            pytest.param(
                [],
                BuildParameters(organization_dir=Path("non_existent_org")),
                ["cdf", "build", "-o", "non_existent_org"],
                "Organization directory 'non_existent_org' not found",
                id="Organization directory does not exist",
            ),
            pytest.param(
                [f"org/{MODULES}/config.dev.yaml"],  # Note: logic below handles suffix -> file, else dir.
                # This path looks like file. But expected struct is org/modules/ AND org/config.yaml.
                # Actually Config file is at org/config.name.yaml.
                # I should just specify list of paths to create.
                # If I put "org/modules/" it has no suffix so created as dir.
                BuildParameters(organization_dir=Path("org"), config_yaml_name="dev"),
                ["cdf", "build", "-o", "org"],
                "Config YAML file 'org/config.dev.yaml' not found",
                id="Config YAML file not found",
            ),
            pytest.param(
                ["org/"],
                BuildParameters(organization_dir=Path("org")),
                ["cdf", "build", "-o", "org"],
                "Could not find the modules directory",
                id="Modules directory not found",
            ),
            # Suggestion case
            pytest.param(
                ["org/", f"actual_org/{MODULES}/"],
                BuildParameters(organization_dir=Path("org")),
                ["cdf", "build", "-o", "org"],
                "Could not find the modules directory",
                id="Modules directory not found with suggestion",
            ),
            pytest.param(
                [f"org/{MODULES}/"],
                BuildParameters(organization_dir=Path("org")),
                ["cdf", "build", "-o", "org"],
                None,
                id="Success with no config",
            ),
            pytest.param(
                [f"org/{MODULES}/", "org/config.dev.yaml"],
                BuildParameters(organization_dir=Path("org"), config_yaml_name="dev"),
                ["cdf", "build", "-o", "org"],
                None,
                id="Success with config",
            ),
        ],
    )
    def test_validate_build_parameters(
        self,
        paths: list[str],
        parameters: BuildParameters,
        user_args: list[str],
        expected_error: str | None,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        # Set up the organization directory and config file if needed
        for path_str in paths:
            path = Path(path_str)
            if path.suffix:  # If the path has a suffix, it's a file
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            else:
                path.mkdir(parents=True, exist_ok=True)

        console = MagicMock(spec=Console)
        if expected_error:
            with pytest.raises(ToolkitError) as exc_info:
                BuildV2Command.validate_build_parameters(parameters, console, user_args)

            assert expected_error in str(exc_info.value).replace("\\", "/")  # Normalize paths for windows
        else:
            BuildV2Command.validate_build_parameters(parameters, console, user_args)


@pytest.fixture(scope="session")
def relative_path_adapter() -> TypeAdapter[RelativeDirPath]:
    return TypeAdapter(RelativeDirPath)


class TestRelativeDirPath:
    @pytest.mark.parametrize(
        "input_path, is_relative, error",
        [
            pytest.param("org/modules", False, "is not a relative path", id="Relative path without leading dot"),
            pytest.param("org/modules", True, "", id="Relative path with leading dot"),
            pytest.param("org/file.yaml", True, "is not a directory", id="Path with file suffix"),
        ],
    )
    def test_relative_dir_path(
        self,
        input_path: str,
        is_relative: bool,
        error: str,
        tmp_path: Path,
        relative_path_adapter: TypeAdapter[RelativeDirPath],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        test_path = tmp_path / input_path
        if test_path.suffix:  # If the path has a suffix, create it as a file
            test_path.parent.mkdir(parents=True, exist_ok=True)
            test_path.touch()
        else:
            test_path.mkdir(parents=True, exist_ok=True)
        if is_relative:
            test_path = test_path.relative_to(tmp_path)

        if error:
            with pytest.raises(ValueError) as exc_info:
                relative_path_adapter.validate_python(test_path)
            assert error in str(exc_info.value)
        else:
            result = relative_path_adapter.validate_python(test_path)
            assert result == Path(input_path)
