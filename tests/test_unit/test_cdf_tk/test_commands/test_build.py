from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes.data_modeling import DataModelId

from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.data_classes import BuildVariables, Environment
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitMissingModuleError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.loaders import TransformationLoader
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from tests import data


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        build_type="dev",
        selected=["none"],
    )


class TestBuildCommand:
    def test_module_not_found_error(self, tmp_path: Path) -> None:
        with pytest.raises(ToolkitMissingModuleError):
            BuildCommand(print_warning=False).execute(
                verbose=False,
                build_dir=tmp_path,
                organization_dir=data.PROJECT_WITH_BAD_MODULES,
                selected=None,
                build_env_name="no_module",
                no_clean=False,
            )

    def test_module_with_non_resource_directories(self, tmp_path: Path) -> None:
        cmd = BuildCommand(print_warning=False)
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            organization_dir=data.PROJECT_WITH_BAD_MODULES,
            selected=None,
            build_env_name="ill_module",
            no_clean=False,
        )

        assert len(cmd.warning_list) >= 1
        assert (
            LowSeverityWarning(
                f"Module 'ill_made_module' has non-resource directories: ['spaces']. {ModuleDefinition.short()}"
            )
            in cmd.warning_list
        )

    def test_custom_project_no_warnings(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        cmd = BuildCommand(print_warning=False)
        monkeypatch.setenv("CDF_PROJECT", "some-project")
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            organization_dir=data.PROJECT_NO_COGNITE_MODULES,
            selected=None,
            build_env_name="dev",
            no_clean=False,
        )

        assert not cmd.warning_list, f"No warnings should be raised. Got warnings: {cmd.warning_list}"
        # There are two transformations in the project, expect two transformation files
        transformation_files = [
            f
            for f in (tmp_path / "transformations").iterdir()
            if f.is_file() and TransformationLoader.is_supported_file(f)
        ]
        assert len(transformation_files) == 2


class TestCheckYamlSemantics:
    def test_build_valid_read_int_version(self) -> None:
        cmd = BuildCommand(silent=True)
        raw_yaml = """destination:
  dataModel:
    destinationType: CogniteFile
    externalId: MyModel
    space: my_space
    version: 1_0_0
  instanceSpace: my_space
  type: instances
externalId: some_external_id
    """
        source_filepath = MagicMock(spec=Path)
        source_filepath.read_text.return_value = raw_yaml
        source_filepath.suffix = ".yaml"

        source_files = cmd._replace_variables(
            [source_filepath], BuildVariables([]), TransformationLoader.folder_name, Path("my_module"), verbose=False
        )
        assert len(source_files) == 1
        source_file = source_files[0]
        assert isinstance(source_file.loaded, dict)
        actual = DataModelId.load(source_file.loaded["destination"]["dataModel"])
        assert actual == DataModelId("my_space", "MyModel", "1_0_0")
