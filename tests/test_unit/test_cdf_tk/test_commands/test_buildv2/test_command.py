from pathlib import Path
from unittest.mock import MagicMock

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters, RelativeDirPath
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import SpaceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitValueError


def create_space_resource_file(organization_dir: Path) -> Path:
    resource_file = organization_dir / MODULES / "my_module" / SpaceCRUD.folder_name / f"my_space.{SpaceCRUD.kind}.yaml"
    resource_file.parent.mkdir(parents=True)
    space_yaml = """space: my_space
name: My Space
"""
    resource_file.write_text(space_yaml)
    return resource_file


class TestBuildCommand:
    def test_end_to_end(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        # Set up a simple organization with modules folder.
        org = tmp_path / "org"
        resource_file = create_space_resource_file(org)

        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)

        folder = cmd.build(parameters)

        assert folder

        built_space = list(build_dir.rglob(f"*.{SpaceCRUD.kind}.yaml"))
        assert len(built_space) == 1
        assert built_space[0].read_text() == resource_file.read_text()

        assert SpaceCRUD.folder_name in folder.resource_by_type
        assert str(folder.resource_by_type[SpaceCRUD.folder_name][SpaceCRUD.kind][0]) == str(built_space[0])
        assert len(folder.insights) == 1
        assert isinstance(folder.insights[0], Recommendation)

    def test_end_to_end_failed_build(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        # Set up a simple organization with modules folder.
        org = tmp_path / "org"
        resource_file = org / "modules" / "my_module" / SpaceCRUD.folder_name / f"my_space.{SpaceCRUD.kind}.yaml"
        resource_file.parent.mkdir(parents=True)
        space_yaml = """space: my#space
name: My Space
"""
        resource_file.write_text(space_yaml)
        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)

        folder = cmd.build(parameters)

        assert folder.resource_by_type == {}
        assert len(folder.insights) == 1


class TestValidateBuildParameters:
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


class TestReadParameters:
    def test_happy_path(self, tmp_path: Path) -> None:
        config_yaml = tmp_path / "config.dev.yaml"
        config_yaml.write_text("""environment:
  name: dev
  project: my-project
  validation-type: dev
  selected:
  - modules/ignore_selection
""")
        resource_file = create_space_resource_file(tmp_path)

        parameters = BuildParameters(
            organization_dir=tmp_path,
            build_dir=Path("build"),
            config_yaml_name="dev",
            user_selected_modules=["module1", "module2"],
        )
        parse_input = BuildV2Command.read_parameters(parameters)
        assert parse_input.model_dump() == {
            "yaml_files": [resource_file],
            # Since user_selected_modules are provided, they should be used instead of config selected modules.
            "selected_modules": {"module1", "module2"},
            "variables": {},
            "validation_type": "dev",
            "cdf_project": "my-project",
        }

    def test_invalid_config_yaml(self, tmp_path: Path) -> None:
        config_yaml = tmp_path / "config.dev.yaml"
        config_yaml.write_text("""environment:
    name: dev
    project: my-project
    validation-type: invalid_type
    selected:
    - modules/
""")
        _ = create_space_resource_file(tmp_path)
        parameters = BuildParameters(organization_dir=tmp_path, build_dir=Path("build"), config_yaml_name="dev")
        with pytest.raises(ToolkitValueError) as exc_info:
            BuildV2Command.read_parameters(parameters)

        assert "In environment.validation-type input should be 'dev' or 'prod'. Got 'invalid_type'." in str(
            exc_info.value
        )

    @pytest.mark.parametrize(
        "paths, user_selection, selection, errors",
        [
            pytest.param(
                ["modules/module1", "modules/module2"],
                ["modules/"],
                {Path("modules")},
                [],
                id="User selects module paths",
            ),
            pytest.param(
                ["modules/module1"],
                ["non_existent/module"],
                set(),
                ["Selected module path 'non_existent/module' does not exist under the organization directory"],
                id="Path does not exist",
            ),
            pytest.param(
                ["modules/module1"],
                ["modules/module1", "non_existent/path"],
                {Path("modules/module1")},
                ["Selected module path 'non_existent/path' does not exist under the organization directory"],
                id="Mix of valid and non-existent paths",
            ),
        ],
    )
    def test_parsing_user_selected_modules(
        self,
        paths: list[str],
        user_selection: list[str],
        selection: set[RelativeDirPath | str],
        errors: list[str],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        for path in paths:
            (tmp_path / Path(path)).mkdir(parents=True, exist_ok=True)

        actual_selection, actual_errors = BuildV2Command._parse_user_selection(user_selection, tmp_path)

        assert actual_errors == errors
        assert actual_selection == selection
