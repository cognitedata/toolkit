import shutil
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
import requests
import yaml
from cognite.client.data_classes import (
    FileMetadata,
    Transformation,
    TransformationSchedule,
)
from cognite.client.data_classes.data_modeling import Edge, Node
from cognite.client.data_classes.hosted_extractors import Destination
from pytest import MonkeyPatch
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk._parameters import ParameterSet, read_parameters_from_dict
from cognite_toolkit._cdf_tk._parameters.data_classes import ParameterSpecSet
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client.data_classes.graphql_data_models import GraphQLDataModel
from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand, ModulesCommand
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuildEnvironment,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
    DatapointsLoader,
    FileMetadataLoader,
    FunctionScheduleLoader,
    GroupResourceScopedLoader,
    HostedExtractorDestinationLoader,
    Loader,
    LocationFilterLoader,
    ResourceLoader,
    ResourceTypes,
    ViewLoader,
    get_loader,
)
from cognite_toolkit._cdf_tk.loaders._resource_loaders.workflow_loaders import WorkflowTriggerLoader
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    tmp_build_directory,
)
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml
from tests.constants import REPO_ROOT
from tests.data import LOAD_DATA, PROJECT_FOR_TEST, RESOURCES_WITH_ENVIRONMENT_VARIABLES
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.test_cdf_tk.constants import BUILD_DIR, SNAPSHOTS_DIR_ALL
from tests.test_unit.utils import FakeCogniteResourceGenerator

SNAPSHOTS_DIR = SNAPSHOTS_DIR_ALL / "load_data_snapshots"


@pytest.mark.parametrize(
    "loader_cls",
    [
        FileMetadataLoader,
        DatapointsLoader,
    ],
)
def test_loader_class(
    loader_cls: type[ResourceLoader],
    toolkit_client_approval: ApprovalToolkitClient,
    data_regression: DataRegressionFixture,
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
    cdf_tool.client = toolkit_client_approval.mock_client
    cdf_tool.toolkit_client = toolkit_client_approval.mock_client
    cdf_tool.data_set_id = 999

    cmd = DeployCommand(print_warning=False)
    loader = loader_cls.create_loader(cdf_tool, LOAD_DATA)
    cmd.deploy_resources(loader, cdf_tool, BuildEnvironment(), dry_run=False)

    dump = toolkit_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{loader.folder_name}.yaml")


def has_auth(params: ParameterSpecSet) -> bool:
    for param in params:
        path_segments = param.path if isinstance(param.path, (list, tuple)) else [param.path]
        if (
            any("authentication" in segment for segment in path_segments)
            or any("credentials" in segment for segment in path_segments)
            or any("secrets" in segment for segment in path_segments)
            or any("envVars" in segment for segment in path_segments)
        ):
            return True
    return False


class TestDeployResources:
    def test_deploy_resource_order(self, toolkit_client_approval: ApprovalToolkitClient):
        build_env_name = "dev"
        cdf_toml = CDFToml.load(PROJECT_FOR_TEST)
        config = BuildConfigYAML.load_from_directory(PROJECT_FOR_TEST, build_env_name)
        config.environment.selected = ["another_module"]
        build_cmd = BuildCommand()
        build_cmd.build_config(
            BUILD_DIR, PROJECT_FOR_TEST, config=config, packages=cdf_toml.modules.packages, clean=True, verbose=False
        )
        expected_order = ["MyView", "MyOtherView"]
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
        cdf_tool.client = toolkit_client_approval.mock_client
        cdf_tool.toolkit_client = toolkit_client_approval.mock_client

        cmd = DeployCommand(print_warning=False)
        cmd.deploy_resources(ViewLoader.create_loader(cdf_tool, BUILD_DIR), cdf_tool, BuildEnvironment(), dry_run=False)

        views = toolkit_client_approval.dump(sort=False)["View"]

        actual_order = [view["externalId"] for view in views]

        assert actual_order == expected_order


class TestFormatConsistency:
    @pytest.mark.parametrize("Loader", RESOURCE_LOADER_LIST)
    def test_fake_resource_generator(
        self, Loader: type[ResourceLoader], cdf_tool_mock: CDFToolConfig, monkeypatch: MonkeyPatch
    ):
        fakegenerator = FakeCogniteResourceGenerator(seed=1337)

        loader = Loader.create_loader(cdf_tool_mock, None)
        instance = fakegenerator.create_instance(loader.resource_write_cls)

        assert isinstance(instance, loader.resource_write_cls)

    @pytest.mark.parametrize("Loader", RESOURCE_LOADER_LIST)
    def test_loader_takes_dict(
        self, Loader: type[ResourceLoader], cdf_tool_mock: CDFToolConfig, monkeypatch: MonkeyPatch
    ) -> None:
        loader = Loader.create_loader(cdf_tool_mock, None)

        if loader.resource_cls in [Transformation, FileMetadata, GraphQLDataModel]:
            pytest.skip("Skipped loaders that require secondary files")
        elif loader.resource_cls in [Edge, Node, Destination]:
            pytest.skip(f"Skipping {loader.resource_cls} because it has special properties")
        elif Loader in [GroupResourceScopedLoader]:
            pytest.skip(f"Skipping {loader.resource_cls} because it requires scoped capabilities")
        elif Loader in [LocationFilterLoader]:
            pytest.skip(f"Skipping {loader.resource_cls} because it requires special handling")

        instance = FakeCogniteResourceGenerator(seed=1337).create_instance(loader.resource_write_cls)

        # special case
        if isinstance(instance, TransformationSchedule):
            del instance.id  # Client validation does not allow id and externalid to be set simultaneously

        file = MagicMock(spec=Path)
        file.read_text.return_value = yaml.dump(instance.dump())
        file.suffix = ".yaml"
        file.name = "dict.yaml"
        file.parent.name = loader.folder_name

        loaded = loader.load_resource(filepath=file, ToolGlobals=cdf_tool_mock, skip_validation=True)
        assert isinstance(
            loaded, (loader.resource_write_cls, loader.list_write_cls)
        ), f"loaded must be an instance of {loader.list_write_cls} or {loader.resource_write_cls} but is {type(loaded)}"

    @pytest.mark.parametrize("Loader", RESOURCE_LOADER_LIST)
    def test_loader_takes_list(
        self, Loader: type[ResourceLoader], cdf_tool_mock: CDFToolConfig, monkeypatch: MonkeyPatch
    ) -> None:
        loader = Loader.create_loader(cdf_tool_mock, None)

        if loader.resource_cls in [Transformation, FileMetadata, GraphQLDataModel]:
            pytest.skip("Skipped loaders that require secondary files")
        elif loader.resource_cls in [Edge, Node, Destination]:
            pytest.skip(f"Skipping {loader.resource_cls} because it has special properties")
        elif Loader in [GroupResourceScopedLoader]:
            pytest.skip(f"Skipping {loader.resource_cls} because it requires scoped capabilities")
        elif Loader in [LocationFilterLoader]:
            # TODO: https://cognitedata.atlassian.net/browse/CDF-22363
            pytest.skip(
                f"Skipping {loader.resource_cls} because FakeCogniteResourceGenerator doesn't generate cls properties correctly"
            )

        instances = FakeCogniteResourceGenerator(seed=1337).create_instances(loader.list_write_cls)

        # special case
        if isinstance(loader.resource_cls, TransformationSchedule):
            for instance in instances:
                del instance.id  # Client validation does not allow id and externalid to be set simultaneously

        file = MagicMock(spec=Path)
        file.read_text.return_value = yaml.dump(instances.dump())
        file.suffix = ".yaml"
        file.name = "dict.yaml"
        file.parent.name = loader.folder_name

        loaded = loader.load_resource(filepath=file, ToolGlobals=cdf_tool_mock, skip_validation=True)
        assert isinstance(
            loaded, (loader.resource_write_cls, loader.list_write_cls)
        ), f"loaded must be an instance of {loader.list_write_cls} or {loader.resource_write_cls} but is {type(loaded)}"

    @staticmethod
    def check_url(url) -> bool:
        try:
            response = requests.get(url, allow_redirects=True)
            return response.status_code >= 200 and response.status_code <= 300
        except requests.exceptions.RequestException:
            return False

    @pytest.mark.parametrize(
        "Loader", [loader for loader in LOADER_LIST if loader.folder_name != "robotics"]
    )  # Robotics does not have a public doc_url
    def test_loader_has_doc_url(self, Loader: type[Loader], cdf_tool_mock: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = Loader.create_loader(cdf_tool_mock, None)
        assert loader.doc_url() != loader._doc_base_url, f"{Loader.folder_name} is missing doc_url deep link"
        assert self.check_url(loader.doc_url()), f"{Loader.folder_name} doc_url is not accessible"


def test_resource_types_is_up_to_date() -> None:
    expected = set(LOADER_BY_FOLDER_NAME.keys())
    actual = set(ResourceTypes.__args__)

    missing = expected - actual
    extra = actual - expected
    assert not missing, f"Missing {missing=}"
    assert not extra, f"Extra {extra=}"


@contextmanager
def tmp_org_directory() -> Iterator[Path]:
    org_dir = Path(tempfile.mkdtemp(prefix="orgdir.", suffix=".tmp", dir=Path.cwd()))
    try:
        yield org_dir
    finally:
        shutil.rmtree(org_dir)


def cognite_module_files_with_loader() -> Iterable[ParameterSet]:
    with tmp_org_directory() as organization_dir, tmp_build_directory() as build_dir:
        ModulesCommand().init(organization_dir, select_all=True, clean=True)
        cdf_toml = CDFToml.load(REPO_ROOT)
        config = BuildConfigYAML.load_from_directory(organization_dir, "dev")
        config.set_environment_variables()
        # Use path syntax to select all modules in the source directory
        config.environment.selected = [Path()]

        built_modules = BuildCommand().build_config(
            build_dir=build_dir,
            organization_dir=organization_dir,
            config=config,
            packages=cdf_toml.modules.packages,
            clean=True,
            verbose=False,
        )
        for module in built_modules:
            for resource_folder, resources in module.resources.items():
                for resource in resources:
                    try:
                        loader = get_loader(resource_folder, resource.kind)
                    except ValueError:
                        # Cannot find loader for resource kind
                        continue
                    filepath = cast(Path, resource.destination)
                    if issubclass(loader, ResourceLoader):
                        raw = yaml.CSafeLoader(filepath.read_text()).get_data()

                        if isinstance(raw, dict):
                            yield pytest.param(loader, raw, id=f"{module.name} - {filepath.stem} - dict")
                        elif isinstance(raw, list):
                            for no, item in enumerate(raw):
                                yield pytest.param(loader, item, id=f"{module.name} - {filepath.stem} - list {no}")


class TestResourceLoaders:
    @pytest.mark.parametrize("loader_cls", RESOURCE_LOADER_LIST)
    def test_get_write_cls_spec(self, loader_cls: type[ResourceLoader]) -> None:
        resource = FakeCogniteResourceGenerator(seed=1337, max_list_dict_items=1).create_instance(
            loader_cls.resource_write_cls
        )
        resource_dump = resource.dump(camel_case=True)
        # These are handled by the toolkit
        resource_dump.pop("dataSetId", None)
        resource_dump.pop("targetDataSetId", None)
        resource_dump.pop("fileId", None)
        resource_dump.pop("assetIds", None)
        resource_dump.pop("assetId", None)
        resource_dump.pop("parentId", None)
        dumped = read_parameters_from_dict(resource_dump)
        spec = loader_cls.get_write_cls_parameter_spec()

        for param in list(dumped):
            # Required for Location Filter
            if "dataSetIds" in param.path:
                dumped.discard(param)

        extra = dumped - spec

        # The spec is calculated based on the resource class __init__ method.
        # There can be deviations in the output from the dump. If that is the case,
        # the 'get_write_cls_parameter_spec' must be updated in the loader. See, for example, the DataModelLoader.
        assert sorted(extra) == []

    @pytest.mark.parametrize("loader_cls, content", list(cognite_module_files_with_loader()))
    def test_write_cls_spec_against_cognite_modules(self, loader_cls: type[ResourceLoader], content: dict) -> None:
        spec = loader_cls.get_write_cls_parameter_spec()

        warnings = validate_resource_yaml(content, spec, Path("test.yaml"))

        assert sorted(warnings) == []

    @pytest.mark.parametrize("loader_cls", RESOURCE_LOADER_LIST)
    def test_empty_required_capabilities_when_no_items(
        self, loader_cls: type[ResourceLoader], cdf_tool_mock: CDFToolConfig
    ):
        actual = loader_cls.get_required_capability(loader_cls.list_write_cls([]), read_only=False)

        assert actual == []

    def test_unique_kind_by_folder(self):
        kind = defaultdict(list)
        for loader_cls in RESOURCE_LOADER_LIST:
            kind[loader_cls.folder_name].append(loader_cls.kind)

        duplicated = {folder: Counter(kinds) for folder, kinds in kind.items() if len(set(kinds)) != len(kinds)}
        # we have two types Group loaders, one for scoped and one for all
        # this is intended and thus not an issue.
        duplicated.pop("auth")

        assert not duplicated, f"Duplicated kind by folder: {duplicated!s}"

    @pytest.mark.parametrize("loader_cls", [loader for loader in RESOURCE_LOADER_LIST])
    def test_should_replace_env_var(self, loader_cls) -> None:
        has_auth_params = has_auth(loader_cls.get_write_cls_parameter_spec())

        if has_auth_params:
            assert (
                loader_cls.do_environment_variable_injection
            ), f"{loader_cls.folder_name} has auth but is not set to replace env vars"
        else:
            assert (
                not loader_cls.do_environment_variable_injection
            ), f"{loader_cls.folder_name} has no auth but is set to replace env vars"

    @pytest.mark.parametrize(
        "loader_cls",
        [loader_cls for loader_cls in RESOURCE_LOADER_LIST if has_auth(loader_cls.get_write_cls_parameter_spec())],
    )
    def test_does_replace_env_var(self, loader_cls, cdf_tool_mock: CDFToolConfig, monkeypatch) -> None:
        raw_path = Path(RESOURCES_WITH_ENVIRONMENT_VARIABLES) / "modules" / "example_module" / loader_cls.folder_name

        tmp_file = next((file for file in raw_path.glob(f"*.{loader_cls.kind}.yaml")), None)
        assert tmp_file is not None, f"No yaml file found in {raw_path}"

        monkeypatch.setenv("SOME_VARIABLE", "test_value")
        monkeypatch.setenv("ANOTHER_VARIABLE", "another_test_value")

        loader = loader_cls.create_loader(cdf_tool_mock, None)
        resource = loader.load_resource(tmp_file, ToolGlobals=cdf_tool_mock, skip_validation=True)
        if isinstance(resource, Iterable):
            resource = next(iter(resource))

        # special case: auth object is moved to extra_configs
        if isinstance(loader, FunctionScheduleLoader):
            extras = next(iter(loader.extra_configs.items()))[1]
            assert extras["authentication"]["clientId"] == "test_value"
            assert extras["authentication"]["clientSecret"] == "another_test_value"
        elif isinstance(loader, WorkflowTriggerLoader):
            extras = next(iter(loader._authentication_by_id.items()), None)[1]
            assert extras.client_id == "test_value"
            assert extras.client_secret == "another_test_value"
        elif isinstance(loader, HostedExtractorDestinationLoader):
            pytest.skip(
                "Hosted Extractor Destination Loader converts credentials to nonce using the session API, skipping"
            )
        else:
            txt = str(resource.dump())
            assert "test_value" in txt
            assert "${SOME_VARIABLE}" not in txt


class TestLoaders:
    def test_unique_display_names(self, cdf_tool_mock: CDFToolConfig):
        name_by_count = Counter(
            [loader_cls.create_loader(cdf_tool_mock, None).display_name for loader_cls in LOADER_LIST]
        )

        duplicates = {name: count for name, count in name_by_count.items() if count > 1}

        assert not duplicates, f"Duplicate display names: {duplicates}"
