from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import respx
from rich.console import Console

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ViewId, ViewNoVersionId
from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters, RelativeDirPath
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    FailedReadYAMLFile,
    ModuleId,
    ResourceType,
    SuccessfulReadYAMLFile,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteDirPath, AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitValueError
from cognite_toolkit._cdf_tk.resources_ios import FileMetadataCRUD, SearchConfigIO, SpaceCRUD
from cognite_toolkit._cdf_tk.resources_ios._base_cruds import ResourceIO
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.datamodel import DataModelIO, ViewIO
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.workflow import WorkflowIO
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet

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

FILEMETADATA_YAML = """externalId: my_file
name: the_filename
$FILEPATH: text_file.txt
mimeType: text/plain
"""


def create_resource_file(organization_dir: Path, crud: type[ResourceIO], resource_yaml: str) -> Path:
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
        dm_file = create_resource_file(org, DataModelIO, DM_YAML)
        view_file = create_resource_file(org, ViewIO, VIEW_YAML)
        _ = create_resource_file(org, WorkflowIO, WORKFLOW_YAML)

        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)

        _ = cmd.build(parameters, tlk_client)

        built_space = list(build_dir.rglob(f"*.{SpaceCRUD.kind}.yaml"))
        assert len(built_space) == 1
        assert built_space[0].read_text() == space_file.read_text()

        built_dm = list(build_dir.rglob(f"*.{DataModelIO.kind}.yaml"))
        assert len(built_dm) == 1
        assert built_dm[0].read_text() == dm_file.read_text()

        built_view = list(build_dir.rglob(f"*.{ViewIO.kind}.yaml"))
        assert len(built_view) == 1
        assert built_view[0].read_text() == view_file.read_text()

        lineage_file = list(build_dir.rglob("lineage.yaml"))
        insights_file = list(build_dir.rglob("insights.csv"))

        assert len(lineage_file) == 1
        assert len(insights_file) == 1

    def test_end_to_end_invalid_space_emits_syntax_warning(self, tmp_path: Path, tlk_client: ToolkitClient) -> None:
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

        my_module = next(m for m in folder.built_modules if m.module_id.name == "my_module")
        assert {
            "resource_count": len(my_module.resources),
            "syntax_warnings": len(my_module.syntax_warnings_by_source),
            "insight_codes": {i.code for i in folder.all_insights if i.code},
        } == {
            "resource_count": 1,
            "syntax_warnings": 1,
            "insight_codes": {"MODEL-SYNTAX-WARNING"},
        }

    def test_build_filemetadata_with_content(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        # Set up a simple organization with modules folder.
        org = tmp_path / "org"

        file_metadata = create_resource_file(org, FileMetadataCRUD, FILEMETADATA_YAML)
        source_txt = file_metadata.parent / "text_file.txt"
        expected_content = "this is a text file"
        source_txt.write_text(expected_content)
        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)
        _ = cmd.build(parameters, client=None)

        files = list((build_dir / FileMetadataCRUD.folder_name).iterdir())
        assert len(files) == 2
        file_by_suffix = dict((file.suffix, file) for file in files)
        assert set(file_by_suffix.keys()) == {".txt", ".yaml"}
        assert file_by_suffix[".txt"].read_text() == expected_content


class TestDependencyValidationSearchConfig:
    @staticmethod
    def _minimal_module(tmp_path: Path) -> tuple[BuiltModule, Path, Path]:
        mod_path = tmp_path / "modules" / "my"
        mod_path.mkdir(parents=True)
        source_file = mod_path / "1-x.SearchConfig.yaml"
        source_file.touch()
        build_file = mod_path / "1-x-out.SearchConfig.yaml"
        build_file.touch()
        module = BuiltModule(
            module_id=ModuleId(id=RelativeDirPath(Path("modules/my")), path=AbsoluteDirPath(mod_path.resolve())),
            resources=[],
        )
        return module, source_file, build_file

    def test_search_config_dependency_satisfied_by_local_view(self, tmp_path: Path) -> None:
        module, source_file, build_file = self._minimal_module(tmp_path)
        view_ref = ViewNoVersionId(space="my_space", external_id="View1")
        module.resources.append(
            BuiltResource(
                identifier=ViewId(space="my_space", external_id="View1", version="v1"),
                source_hash="h-view",
                type=ResourceType(resource_folder=ViewIO.folder_name, kind=ViewIO.kind),
                source_path=AbsoluteFilePath(source_file.resolve()),
                build_path=AbsoluteFilePath(build_file.resolve()),
                crud_cls=ViewIO,
                dependencies=set(),
            )
        )
        module.resources.append(
            BuiltResource(
                identifier=view_ref,
                source_hash="h",
                type=ResourceType(
                    resource_folder=SearchConfigIO.folder_name,
                    kind=SearchConfigIO.kind,
                ),
                source_path=AbsoluteFilePath(source_file.resolve()),
                build_path=AbsoluteFilePath(build_file.resolve()),
                crud_cls=SearchConfigIO,
                dependencies={(ViewIO, view_ref)},
            )
        )
        result = list(DependencyRuleSet([module]).validate())
        assert len(result) == 0


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
        "filename, content, crud_class, expected_code",
        [
            pytest.param(
                "nonexistent.Space.yaml",
                None,
                SpaceCRUD,
                "READ-ERROR",
                id="file_read_error",
            ),
            pytest.param(
                "resource.Space.yaml",
                "key: [unclosed",
                SpaceCRUD,
                "YAML-PARSE-ERROR",
                id="yaml_parse_error",
            ),
        ],
    )
    def test_read_resource_file_failed(
        self,
        filename: str,
        content: str | None,
        crud_class: type[ResourceIO],
        expected_code: str,
        tmp_path: Path,
    ) -> None:
        cmd = BuildV2Command()
        resource_file = tmp_path / filename
        if content is not None:
            resource_file.write_text(content)

        result = cmd._read_resource_file(resource_file, crud_class, [])
        assert isinstance(result, FailedReadYAMLFile)
        assert result.code == expected_code

    @pytest.mark.parametrize(
        "filename, content, crud_class, expected_resource_count, has_syntax_warning",
        [
            pytest.param(
                "resource.Space.yaml",
                "space: my_space\nname: My Space\n",
                SpaceCRUD,
                1,
                False,
                id="successful_single_resource",
            ),
            pytest.param(
                "resource.Space.yaml",
                'space: ""\n',
                SpaceCRUD,
                1,
                True,
                id="model_validation_error_yields_syntax_warning",
            ),
            pytest.param(
                "resource.Space.yaml",
                "space: my_space\nextra_field: value\n",
                SpaceCRUD,
                1,
                True,
                id="extra_fields_produces_syntax_warning",
            ),
            pytest.param(
                "resource.Space.yaml",
                "- space: space_one\n- space: space_two\n",
                SpaceCRUD,
                2,
                False,
                id="multiple_resources_in_list",
            ),
        ],
    )
    def test_read_resource_file_success(
        self,
        filename: str,
        content: str | None,
        crud_class: type[ResourceIO],
        expected_resource_count: int,
        has_syntax_warning: bool,
        tmp_path: Path,
    ) -> None:
        cmd = BuildV2Command()
        resource_file = tmp_path / filename
        if content is not None:
            resource_file.write_text(content)

        result = cmd._read_resource_file(resource_file, crud_class, [])
        assert isinstance(result, SuccessfulReadYAMLFile)
        assert len(result.resources) == expected_resource_count
        assert has_syntax_warning == (result.syntax_warning is not None)


class TestFindUnresolvedVariables:
    @pytest.mark.parametrize(
        "content, expected",
        [
            pytest.param(
                """space: '{{ instanceSpace }}'
name: 'Instance space'
description: This space contains data
""",
                ["instanceSpace"],
                id="Single unresolved variable",
            ),
            pytest.param(
                """externalId: '{{ directRelationJob }}'
config:
  state:
    rawDatabase: {{ rawStateDatabase}}
    rawTable: {{ rawStateTable }}
  data:
    annotationSpace: '{{annotationSpace}}'
    directRelationMappings:
      - startNodeView:
          space: {{schemaSpace }}
          externalId: CogniteFile
          version: v1
          directRelationProperty: assets
""",
                ["directRelationJob", "rawStateDatabase", "rawStateTable", "annotationSpace", "schemaSpace"],
                id="Multiple unresolved variables",
            ),
            pytest.param(
                """name: daily-8am-utc
cronExpression: 0 8 * * *
description: 'Run every day at 8am UTC cdf-auth: a353e490'
functionExternalId: fn_first_function
""",
                [],
                id="No unresolved variables",
            ),
        ],
    )
    def test_find_unresolved_variables(self, content: str, expected: list[str]) -> None:
        assert BuildV2Command._find_unresolved_variables(content) == expected
