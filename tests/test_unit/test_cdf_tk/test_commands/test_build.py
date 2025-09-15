from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes.data_modeling import DataModelId, Space

from cognite_toolkit._cdf_tk.commands.build_cmd import BuildCommand
from cognite_toolkit._cdf_tk.cruds import TransformationCRUD
from cognite_toolkit._cdf_tk.data_classes import BuildVariables, Environment
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitMissingModuleError,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests import data
from tests.test_unit.approval_client import ApprovalToolkitClient


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        validation_type="dev",
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

    @pytest.mark.skipif(not Flags.GRAPHQL.is_enabled(), reason="GraphQL schema files will give warnings")
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
            if f.is_file() and TransformationCRUD.is_supported_file(f)
        ]
        assert len(transformation_files) == 2

    def test_build_complete_org_without_warnings(
        self,
        tmp_path: Path,
        env_vars_with_client: EnvironmentVariables,
    ) -> None:
        cmd = BuildCommand(silent=True, skip_tracking=True)
        with patch.dict(
            os.environ,
            {"CDF_PROJECT": env_vars_with_client.CDF_PROJECT, "CDF_CLUSTER": env_vars_with_client.CDF_CLUSTER},
        ):
            cmd.execute(
                verbose=False,
                build_dir=tmp_path / "build",
                organization_dir=data.COMPLETE_ORG,
                selected=None,
                build_env_name="dev",
                no_clean=False,
            )

        assert not cmd.warning_list, (
            f"No warnings should be raised. Got {len(cmd.warning_list)} warnings: {cmd.warning_list}"
        )

    def test_build_no_warnings_when_space_exists_in_cdf(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        my_group = """name: gp_trigger_issue
sourceId: '1234567890123456789'
capabilities:
- dataModelInstancesAcl:
    actions:
    - READ
    scope:
      spaceIdScope:
        spaceIds:
        - existing-space
"""
        filepath = tmp_path / "my_org" / "modules" / "my_module" / "auth" / "my.Group.yaml"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(my_group)

        # Simulate that the space exists in CDF
        toolkit_client_approval.append(Space, Space("existing-space", False, 1, 1, None, None))
        cmd = BuildCommand(silent=True, skip_tracking=True)
        with patch.dict(
            os.environ,
            {"CDF_PROJECT": env_vars_with_client.CDF_PROJECT, "CDF_CLUSTER": env_vars_with_client.CDF_CLUSTER},
        ):
            cmd.execute(
                verbose=False,
                organization_dir=tmp_path / "my_org",
                build_dir=tmp_path / "build",
                selected=None,
                build_env_name=None,
                no_clean=False,
                client=toolkit_client_approval.mock_client,
                on_error="raise",
            )
        assert len(cmd.warning_list) == 0


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
        source_filepath.read_bytes.return_value = raw_yaml.encode("utf-8")

        source_files = cmd._replace_variables(
            [source_filepath], BuildVariables([]), TransformationCRUD.folder_name, Path("my_module"), verbose=False
        )
        assert len(source_files) == 1
        source_file = source_files[0]
        assert isinstance(source_file.loaded, dict)
        actual = DataModelId.load(source_file.loaded["destination"]["dataModel"])
        assert actual == DataModelId("my_space", "MyModel", "1_0_0")
