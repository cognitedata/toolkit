from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import respx
from rich.console import Console

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters, RelativeDirPath
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    FailedReadYAMLFile,
    SuccessfulReadYAMLFile,
)
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import SpaceCRUD
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceContainerCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.workflow import WorkflowCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitValueError

BASE_URL = "http://neat.cognitedata.com"


@pytest.fixture
def example_statistics_response() -> dict:
    """Example DMS statistics API response."""
    return {
        "spaces": {"count": 5, "limit": 100},
        "containers": {"count": 42, "limit": 1000},
        "views": {"count": 123, "limit": 2000},
        "dataModels": {"count": 8, "limit": 500},
        "containerProperties": {"count": 1234, "limit": 100},
        "instances": {
            "edges": 5000,
            "softDeletedEdges": 100,
            "nodes": 10000,
            "softDeletedNodes": 200,
            "instances": 15000,
            "instancesLimit": 5000000,
            "softDeletedInstances": 300,
            "softDeletedInstancesLimit": 10000000,
        },
        "concurrentReadLimit": 10,
        "concurrentWriteLimit": 5,
        "concurrentDeleteLimit": 3,
    }


@pytest.fixture()
def tlk_client(toolkit_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(config=toolkit_config)


@pytest.fixture()
def empty_cdf(
    toolkit_config: ToolkitClientConfig, example_statistics_response: dict, respx_mock: respx.MockRouter
) -> respx.MockRouter:
    config = toolkit_config
    empty_response: dict[str, Any] = {
        "items": [],
        "nextCursor": None,
    }
    for endpoint in [
        "/models/spaces/byids",
        "/models/containers/byids",
        "/models/views/byids",
        "/models/datamodels/byids",
    ]:
        respx_mock.post(
            config.create_api_url(endpoint),
        ).respond(
            status_code=200,
            json=empty_response,
        )

    for endpoint, response in [
        ("/models/containers", empty_response),
        ("/models/views", empty_response),
        ("/models/datamodels", empty_response),
        ("/models/spaces", empty_response),
        ("/models/statistics", example_statistics_response),
    ]:
        respx_mock.get(
            config.create_api_url(endpoint),
        ).respond(
            status_code=200,
            json=response,
        )
    return respx_mock


WORKFLOW_YAML = """externalId: my_workflow"""

SPACE_YAML = """space: my_space
name: My Space
"""

DM_YAML = """space: my_space
externalId: MyModel
version: v1
views:
- type: view
  space: my_space
  externalId: View1
  version: v1
"""

VIEW_YAML = """space: my_space
externalId: View1
version: v1
properties:
  name:
    container:
      type: container
      space: cdm
      externalId: CogniteDescribable
    containerPropertyIdentifier: name
"""


def create_resource_file(organization_dir: Path, crud: type[ResourceContainerCRUD], resource_yaml: str) -> Path:
    resource_file = organization_dir / MODULES / "my_module" / crud.folder_name / f"my_space.{crud.kind}.yaml"
    resource_file.parent.mkdir(parents=True, exist_ok=True)
    resource_file.write_text(resource_yaml)
    return resource_file


@pytest.mark.usefixtures("empty_cdf")
class TestBuildCommand:
    def test_end_to_end(self, tmp_path: Path, tlk_client: ToolkitClient) -> None:
        cmd = BuildV2Command()

        # Set up a simple organization with modules folder.
        org = tmp_path / "org"

        space_file = create_resource_file(org, SpaceCRUD, SPACE_YAML)
        dm_file = create_resource_file(org, DataModelCRUD, DM_YAML)
        view_file = create_resource_file(org, ViewCRUD, VIEW_YAML)
        _ = create_resource_file(org, WorkflowCRUD, WORKFLOW_YAML)

        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)

        _ = cmd.build(parameters, tlk_client)

        built_space = list(build_dir.rglob(f"*.{SpaceCRUD.kind}.yaml"))
        assert len(built_space) == 1
        assert built_space[0].read_text() == space_file.read_text()

        built_dm = list(build_dir.rglob(f"*.{DataModelCRUD.kind}.yaml"))
        assert len(built_dm) == 1
        assert built_dm[0].read_text() == dm_file.read_text()

        built_view = list(build_dir.rglob(f"*.{ViewCRUD.kind}.yaml"))
        assert len(built_view) == 1
        assert built_view[0].read_text() == view_file.read_text()

        lineage_file = list(build_dir.rglob("lineage.yaml"))
        insights_file = list(build_dir.rglob("insights.csv"))

        assert len(lineage_file) == 1
        assert len(insights_file) == 1

    def test_end_to_end_failed_build(self, tmp_path: Path, tlk_client: ToolkitClient) -> None:
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

        folder = cmd.build(parameters, tlk_client)

        assert "my_module" in folder.built_modules_by_success[False]


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
                BuildV2Command._validate_build_parameters(parameters, console, user_args)

            assert expected_error in str(exc_info.value).replace("\\", "/")  # Normalize paths for windows
        else:
            BuildV2Command._validate_build_parameters(parameters, console, user_args)


class TestReadFileSystem:
    def test_happy_path(self, tmp_path: Path) -> None:
        config_yaml = tmp_path / "config.dev.yaml"
        config_yaml.write_text("""environment:
  name: dev
  project: my-project
  validation-type: dev
  selected:
  - modules/ignore_selection
""")
        resource_file = create_resource_file(tmp_path, SpaceCRUD, SPACE_YAML)

        parameters = BuildParameters(
            organization_dir=tmp_path,
            build_dir=Path("build"),
            config_yaml_name="dev",
            user_selected_modules=["module1", "module2"],
        )
        build_files = BuildV2Command._read_file_system(parameters)
        assert build_files.model_dump() == {
            "yaml_files": [resource_file.relative_to(tmp_path)],
            # Since user_selected_modules are provided, they should be used instead of config selected modules.
            "selected_modules": {"module1", "module2"},
            "variables": {},
            "validation_type": "dev",
            "cdf_project": "my-project",
            "organization_dir": tmp_path.resolve(),
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
        _ = create_resource_file(tmp_path, SpaceCRUD, SPACE_YAML)
        parameters = BuildParameters(organization_dir=tmp_path, build_dir=Path("build"), config_yaml_name="dev")
        with pytest.raises(ToolkitValueError) as exc_info:
            BuildV2Command._read_file_system(parameters)

        assert "In environment.validation-type input should be 'dev' or 'prod'. Got 'invalid_type'." in str(
            exc_info.value
        )

    @pytest.mark.parametrize(
        "paths, user_selection, organization_dir, selection, errors",
        [
            pytest.param(
                ["modules/module1"],
                ["modules/"],
                Path("tests/data/complete_org"),
                {Path("modules")},
                [],
                id="Current working directory parent of organization_dir",
            ),
            pytest.param(
                ["modules/module1", "modules/module2"],
                ["modules/"],
                Path("."),
                {Path("modules")},
                [],
                id="User selects module paths",
            ),
            pytest.param(
                ["modules/module1"],
                ["non_existent/module"],
                Path("."),
                set(),
                ["Selected module path 'non_existent/module' does not exist under the organization directory"],
                id="Path does not exist",
            ),
            pytest.param(
                ["modules/module1"],
                ["modules/module1", "non_existent/path"],
                Path("."),
                {Path("modules/module1")},
                ["Selected module path 'non_existent/path' does not exist under the organization directory"],
                id="Mix of valid and non-existent paths",
            ),
            pytest.param(
                ["modules/module1", "modules/module2"],
                ["../../other_org/modules/module3"],
                Path("."),
                set(),
                [
                    "Selected module path '../../other_org/modules/module3' does not exist under the organization directory"
                ],
                id="Attack path outside of organization directory",
            ),
        ],
    )
    def test_parsing_user_selected_modules(
        self,
        paths: list[str],
        user_selection: list[str],
        organization_dir: Path,
        selection: set[RelativeDirPath | str],
        errors: list[str],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)

        organization_path = tmp_path / organization_dir
        for path in paths:
            (organization_path / Path(path)).mkdir(parents=True, exist_ok=True)

        actual_selection, actual_errors = BuildV2Command._parse_user_selection(user_selection, organization_path)

        assert actual_errors == errors
        assert actual_selection == selection


def _read_resource_outcome(result: FailedReadYAMLFile | SuccessfulReadYAMLFile) -> dict[str, Any]:
    if isinstance(result, FailedReadYAMLFile):
        return {
            "outcome": "failed",
            "code": result.code,
            "resource_count": None,
            "has_syntax_warning": None,
        }
    return {
        "outcome": "success",
        "code": None,
        "resource_count": len(result.resources),
        "has_syntax_warning": result.syntax_warning is not None,
    }


class TestReadResourceFile:
    @pytest.mark.parametrize(
        "filename, content, crud_class, expected",
        [
            pytest.param(
                "resource.yaml",
                "space: test\n",
                SpaceCRUD,
                {
                    "outcome": "success",
                    "code": None,
                    "resource_count": 1,
                    "has_syntax_warning": False,
                },
                id="filename_without_kind_suffix_still_parses_when_crud_explicit",
            ),
            pytest.param(
                "nonexistent.Space.yaml",
                None,
                SpaceCRUD,
                {
                    "outcome": "failed",
                    "code": "READ-ERROR",
                    "resource_count": None,
                    "has_syntax_warning": None,
                },
                id="file_read_error",
            ),
            pytest.param(
                "resource.Space.yaml",
                "key: [unclosed",
                SpaceCRUD,
                {
                    "outcome": "failed",
                    "code": "YAML-PARSE-ERROR",
                    "resource_count": None,
                    "has_syntax_warning": None,
                },
                id="yaml_parse_error",
            ),
            pytest.param(
                "resource.Space.yaml",
                "space: my_space\nname: My Space\n",
                SpaceCRUD,
                {
                    "outcome": "success",
                    "code": None,
                    "resource_count": 1,
                    "has_syntax_warning": False,
                },
                id="successful_single_resource",
            ),
            pytest.param(
                "resource.Space.yaml",
                'space: ""\n',
                SpaceCRUD,
                {
                    "outcome": "success",
                    "code": None,
                    "resource_count": 1,
                    "has_syntax_warning": True,
                },
                id="model_validation_error_yields_syntax_warning",
            ),
            pytest.param(
                "resource.Space.yaml",
                "space: my_space\nextra_field: value\n",
                SpaceCRUD,
                {
                    "outcome": "success",
                    "code": None,
                    "resource_count": 1,
                    "has_syntax_warning": True,
                },
                id="extra_fields_produces_syntax_warning",
            ),
            pytest.param(
                "resource.Space.yaml",
                "- space: space_one\n- space: space_two\n",
                SpaceCRUD,
                {
                    "outcome": "success",
                    "code": None,
                    "resource_count": 2,
                    "has_syntax_warning": False,
                },
                id="multiple_resources_in_list",
            ),
        ],
    )
    def test_read_resource_file(
        self,
        filename: str,
        content: str | None,
        crud_class: type[ResourceCRUD],
        expected: dict[str, Any],
        tmp_path: Path,
    ) -> None:
        cmd = BuildV2Command()
        resource_file = tmp_path / filename
        if content is not None:
            resource_file.write_text(content)

        result = cmd._read_resource_file(resource_file, crud_class, [])

        assert _read_resource_outcome(result) == expected
