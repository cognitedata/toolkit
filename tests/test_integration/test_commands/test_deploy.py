import difflib
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from cognite.client.data_classes.data_modeling import NodeId
from mypy.checkexpr import defaultdict
from pydantic import JsonValue
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import HTTPResult2, RequestMessage2, SuccessResponse2
from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand, PullCommand
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_LIST,
    FunctionCRUD,
    FunctionScheduleCRUD,
    GraphQLCRUD,
    HostedExtractorDestinationCRUD,
    HostedExtractorSourceCRUD,
    ResourceCRUD,
    ResourceWorker,
    SearchConfigCRUD,
    StreamlitCRUD,
    TransformationCRUD,
    WorkflowTriggerCRUD,
)
from cognite_toolkit._cdf_tk.cruds._resource_cruds.location import LocationFilterCRUD
from cognite_toolkit._cdf_tk.data_classes import BuiltModuleList, ResourceDeployResult
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import remove_trailing_newline
from tests import data
from tests_smoke.exceptions import EndpointAssertionError

# This much match the simulatorExternalId in the SimulatorModel definition of the
# complete_org/complete_org_alpha test cases.
SIMULATOR_EXTERNAL_ID = "integration-test-simulator"


@pytest.fixture(scope="session")
def simulator(toolkit_client: ToolkitClient) -> str:
    """Toolkit does not support simulator creation yet, but we support
    simulator models. Thus, we need this fixture to ensure a simulator exists for
    the simulator model to reference.
    """
    http_client = toolkit_client.http_client
    config = toolkit_client.config
    # Check if simulator already exists
    list_response = http_client.request_single_retries(
        RequestMessage2(
            endpoint_url=config.create_api_url("/simulators/list"),
            method="POST",
            body_content={"limit": 1000},
        )
    )
    if simulator_external_id := _parse_simulator_response(list_response):
        return simulator_external_id

    creation_response = http_client.request_single_retries(
        RequestMessage2(
            endpoint_url=config.create_api_url("/simulators"),
            method="POST",
            body_content={"items": [SIMULATOR]},
        )
    )
    simulator_external_id = _parse_simulator_response(creation_response)
    if simulator_external_id is not None:
        return simulator_external_id
    raise EndpointAssertionError("/simulators", "Failed to create simulator for testing.")


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
@pytest.mark.usefixtures("simulator")
def test_deploy_complete_org(env_vars: EnvironmentVariables, build_dir: Path) -> None:
    build = BuildCommand(silent=True, skip_tracking=True)

    built_modules = build.execute(
        verbose=False,
        organization_dir=data.COMPLETE_ORG,
        build_dir=build_dir,
        build_env_name="dev",
        no_clean=False,
        selected=None,
        client=env_vars.get_client(),
    )

    deploy_command = DeployCommand(silent=False, skip_tracking=True)
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    with patch.dict(
        os.environ,
        {"EVENTHUB_CLIENT_ID": client_id, "EVENTHUB_CLIENT_SECRET": client_secret},
    ):
        deploy_command.deploy_build_directory(
            env_vars=env_vars,
            build_dir=build_dir,
            build_env_name="dev",
            dry_run=False,
            drop=False,
            drop_data=False,
            force_update=False,
            include=None,
            verbose=True,
        )

    changed_resources = get_changed_resources(env_vars, build_dir)
    assert not changed_resources, "Redeploying the same resources should not change anything"

    changed_source_files = get_changed_source_files(env_vars, build_dir, built_modules, verbose=True)
    assert len(changed_source_files) == 0, f"Pulling the same source should not change anything {changed_source_files}"


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
@pytest.mark.usefixtures("simulator")
def test_deploy_complete_org_alpha(env_vars: EnvironmentVariables, build_dir: Path) -> None:
    build = BuildCommand(silent=True, skip_tracking=True)

    built_modules = build.execute(
        verbose=False,
        organization_dir=data.COMPLETE_ORG_ALPHA_FLAGS,
        build_dir=build_dir,
        build_env_name="dev",
        no_clean=False,
        selected=None,
        client=env_vars.get_client(),
    )

    deploy_command = DeployCommand(silent=False, skip_tracking=True)
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    with patch.dict(
        os.environ,
        {"EVENTHUB_CLIENT_ID": client_id, "EVENTHUB_CLIENT_SECRET": client_secret},
    ):
        deploy_command.deploy_build_directory(
            env_vars,
            build_dir=build_dir,
            build_env_name="dev",
            dry_run=False,
            drop=False,
            drop_data=False,
            force_update=False,
            include=None,
            verbose=True,
        )

    changed_resources = get_changed_resources(env_vars, build_dir)
    assert not changed_resources, "Redeploying the same resources should not change anything"

    changed_source_files = get_changed_source_files(env_vars, build_dir, built_modules, verbose=False)
    assert not changed_source_files, "Pulling the same source should not change anything"


def get_changed_resources(env_vars: EnvironmentVariables, build_dir: Path) -> dict[str, set[Any]]:
    changed_resources: dict[str, set[Any]] = {}
    client = env_vars.get_client()
    for loader_cls in RESOURCE_CRUD_LIST:
        if loader_cls in {HostedExtractorSourceCRUD, HostedExtractorDestinationCRUD}:
            # These resources we have no way of knowing if they have changed. So they are always redeployed.
            continue
        loader = loader_cls.create_loader(client, build_dir)
        worker = ResourceWorker(loader, "deploy")
        files = worker.load_files()
        resources = worker.prepare_resources(files, environment_variables=env_vars.dump())
        if changed := (set(loader.get_ids(resources.to_update)) - {NodeId("sp_nodes", "MyExtendedFile")}):
            # We do not have a way to get CogniteFile extensions. This is a workaround to avoid the test failing.
            changed_resources[loader.display_name] = changed

    return changed_resources


def get_changed_source_files(
    env_vars: EnvironmentVariables, build_dir: Path, built_modules: BuiltModuleList, verbose: bool = False
) -> dict[str, set[Path]]:
    # This is a modified copy of the PullCommand._pull_build_dir and PullCommand._pull_resources methods
    # This will likely be hard to maintain, but if the pull command changes, should be refactored to be more
    # maintainable.
    cmd = PullCommand(silent=True, skip_tracking=True)
    changed_source_files: dict[str, set[str]] = defaultdict(set)
    selected_loaders = cmd._clean_command.get_selected_loaders(build_dir, read_resource_folders=set(), include=None)
    for loader_cls in selected_loaders:
        if (not issubclass(loader_cls, ResourceCRUD)) or (
            # Authentication that causes the diff to fail
            loader_cls in {HostedExtractorSourceCRUD, HostedExtractorDestinationCRUD}
            # External files that cannot (or not yet supported) be pulled
            or loader_cls in {GraphQLCRUD, FunctionCRUD, StreamlitCRUD}
            # Have authentication hashes that is different for each environment
            or loader_cls in {TransformationCRUD, FunctionScheduleCRUD, WorkflowTriggerCRUD}
            # LocationFilterLoader needs to split the file into multiple files, so we cannot compare them
            or loader_cls is LocationFilterCRUD
            # SearchConfigLoader is not supported in pull and post that also will require special handling
            or loader_cls is SearchConfigCRUD
        ):
            continue
        loader = loader_cls.create_loader(env_vars.get_client(), build_dir)
        resources = built_modules.get_resources(
            None, loader.folder_name, loader.kind, is_supported_file=loader.is_supported_file
        )
        if not resources:
            continue
        cdf_resources = loader.retrieve(resources.identifiers)
        cdf_resource_by_id = {loader.get_id(r): r for r in cdf_resources}

        resources_by_file = resources.by_file()
        file_results = ResourceDeployResult(loader.display_name)

        environment_variables = env_vars.dump()

        for source_file, resources in resources_by_file.items():
            if source_file.name == "extended.CogniteFile.yaml":
                # The extension of CogniteFile is not yet supported in Toolkit even though we have a test case for it.
                continue
            original_content = remove_trailing_newline(source_file.read_text())
            if "$FILENAME" in original_content:
                # File expansion pattern are not supported in pull.
                continue
            local_resource_by_id = cmd._get_local_resource_dict_by_id(resources, loader, environment_variables)
            _, to_write = cmd._get_to_write(local_resource_by_id, cdf_resource_by_id, file_results, loader)

            new_content, extra_files = cmd._to_write_content(
                source=original_content,
                to_write=to_write,
                resources=resources,
                environment_variables=environment_variables,
                loader=loader,
                source_file=source_file,
            )
            new_content = remove_trailing_newline(new_content)
            if new_content != original_content:
                if verbose:
                    print(
                        Panel(
                            "\n".join(difflib.unified_diff(original_content.splitlines(), new_content.splitlines())),
                            title=f"Diff for {source_file.name}",
                        )
                    )
                changed_source_files[loader.display_name].add(f"{loader.folder_name}/{source_file.name}")
            for path, new_extra_content in extra_files.items():
                new_extra_content = remove_trailing_newline(new_extra_content)
                original_extra_content = remove_trailing_newline(path.read_text(encoding="utf-8"))
                if new_extra_content != original_extra_content:
                    if verbose:
                        print(
                            Panel(
                                "\n".join(
                                    difflib.unified_diff(original_content.splitlines(), new_content.splitlines())
                                ),
                                title=f"Diff for {path.name}",
                            )
                        )
                    changed_source_files[loader.display_name].add(f"{loader.folder_name}/{path.name}")

    return dict(changed_source_files)


def _parse_simulator_response(list_response: HTTPResult2) -> str | None:
    if not isinstance(list_response, SuccessResponse2):
        raise EndpointAssertionError("/simulators/list", str(list_response))
    try:
        items = list_response.body_json["items"]
    except (KeyError, ValueError):
        raise AssertionError(f"Unexpected response format from /simulators/list: {list_response.body}") from None
    try:
        return next(item["externalId"] for item in items if item["externalId"] == SIMULATOR_EXTERNAL_ID)
    except (KeyError, StopIteration):
        return None


SIMULATOR: dict[str, JsonValue] = {
    "name": "Integration Test Simulator",
    "externalId": SIMULATOR_EXTERNAL_ID,
    "fileExtensionTypes": ["txt"],
    "modelTypes": [{"name": "Steady State", "key": "SteadyState"}],
    "modelDependencies": [
        {
            "fileExtensionTypes": ["txt", "xml"],
            "fields": [
                {"name": "fieldA", "label": "label fieldA", "info": "info fieldA"},
                {"name": "fieldB", "label": "label fieldB", "info": "info fieldB"},
            ],
        },
    ],
    "stepFields": [
        {
            "stepType": "get/set",
            "fields": [
                {
                    "name": "objectName",
                    "label": "Simulation Object Name",
                    "info": "Enter the name of the DWSIM object, i.e. Feed",
                },
                {
                    "name": "objectProperty",
                    "label": "Simulation Object Property",
                    "info": "Enter the property of the DWSIM object, i.e. Temperature",
                },
            ],
        },
        {
            "stepType": "command",
            "fields": [
                {
                    "name": "command",
                    "label": "Command",
                    "info": "Select a command",
                    "options": [{"label": "Solve Flowsheet", "value": "Solve"}],
                }
            ],
        },
    ],
    "unitQuantities": [
        {
            "name": "mass",
            "label": "Mass",
            "units": [{"label": "kg", "name": "kg"}, {"label": "g", "name": "g"}, {"label": "lb", "name": "lb"}],
        },
        {
            "name": "time",
            "label": "Time",
            "units": [{"label": "s", "name": "s"}, {"label": "min.", "name": "min."}, {"label": "h", "name": "h"}],
        },
        {
            "name": "accel",
            "label": "Acceleration",
            "units": [
                {"label": "m/s2", "name": "m/s2"},
                {"label": "cm/s2", "name": "cm/s2"},
                {"label": "ft/s2", "name": "ft/s2"},
            ],
        },
        {
            "name": "force",
            "label": "Force",
            "units": [
                {"label": "N", "name": "N"},
                {"label": "dyn", "name": "dyn"},
                {"label": "kgf", "name": "kgf"},
                {"label": "lbf", "name": "lbf"},
            ],
        },
        {
            "name": "volume",
            "label": "Volume",
            "units": [
                {"label": "m3", "name": "m3"},
                {"label": "cm3", "name": "cm3"},
                {"label": "L", "name": "L"},
                {"label": "ft3", "name": "ft3"},
                {"label": "bbl", "name": "bbl"},
                {"label": "gal[US]", "name": "gal[US]"},
                {"label": "gal[UK]", "name": "gal[UK]"},
            ],
        },
        {
            "name": "density",
            "label": "Density",
            "units": [
                {"label": "kg/m3", "name": "kg/m3"},
                {"label": "g/cm3", "name": "g/cm3"},
                {"label": "lbm/ft3", "name": "lbm/ft3"},
            ],
        },
        {
            "name": "diameter",
            "label": "Diameter",
            "units": [{"label": "mm", "name": "mm"}, {"label": "in", "name": "in"}],
        },
        {
            "name": "distance",
            "label": "Distance",
            "units": [{"label": "m", "name": "m"}, {"label": "ft", "name": "ft"}, {"label": "cm", "name": "cm"}],
        },
        {
            "name": "heatflow",
            "label": "Heat Flow",
            "units": [
                {"label": "kW", "name": "kW"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "BTU/h", "name": "BTU/h"},
                {"label": "BTU/s", "name": "BTU/s"},
                {"label": "cal/s", "name": "cal/s"},
                {"label": "HP", "name": "HP"},
                {"label": "kJ/h", "name": "kJ/h"},
                {"label": "kJ/d", "name": "kJ/d"},
                {"label": "MW", "name": "MW"},
                {"label": "W", "name": "W"},
                {"label": "BTU/d", "name": "BTU/d"},
                {"label": "MMBTU/d", "name": "MMBTU/d"},
                {"label": "MMBTU/h", "name": "MMBTU/h"},
                {"label": "kcal/s", "name": "kcal/s"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "kcal/d", "name": "kcal/d"},
            ],
        },
        {
            "name": "pressure",
            "label": "Pressure",
            "units": [
                {"label": "Pa", "name": "Pa"},
                {"label": "atm", "name": "atm"},
                {"label": "kgf/cm2", "name": "kgf/cm2"},
                {"label": "kgf/cm2g", "name": "kgf/cm2g"},
                {"label": "lbf/ft2", "name": "lbf/ft2"},
                {"label": "kPa", "name": "kPa"},
                {"label": "kPag", "name": "kPag"},
                {"label": "bar", "name": "bar"},
                {"label": "barg", "name": "barg"},
                {"label": "ftH2O", "name": "ftH2O"},
                {"label": "inH2O", "name": "inH2O"},
                {"label": "inHg", "name": "inHg"},
                {"label": "mbar", "name": "mbar"},
                {"label": "mH2O", "name": "mH2O"},
                {"label": "mmH2O", "name": "mmH2O"},
                {"label": "mmHg", "name": "mmHg"},
                {"label": "MPa", "name": "MPa"},
                {"label": "psi", "name": "psi"},
                {"label": "psig", "name": "psig"},
            ],
        },
        {
            "name": "velocity",
            "label": "Velocity",
            "units": [
                {"label": "m/s", "name": "m/s"},
                {"label": "cm/s", "name": "cm/s"},
                {"label": "mm/s", "name": "mm/s"},
                {"label": "km/h", "name": "km/h"},
                {"label": "ft/h", "name": "ft/h"},
                {"label": "ft/min", "name": "ft/min"},
                {"label": "ft/s", "name": "ft/s"},
                {"label": "in/s", "name": "in/s"},
            ],
        },
        {
            "name": "temperature",
            "label": "Temperature",
            "units": [
                {"label": "K", "name": "K"},
                {"label": "R", "name": "R"},
                {"label": "C", "name": "C"},
                {"label": "F", "name": "F"},
            ],
        },
        {
            "name": "volumetricFlow",
            "label": "Volumetric Flow",
            "units": [
                {"label": "m3/h", "name": "m3/h"},
                {"label": "cm3/s", "name": "cm3/s"},
                {"label": "L/h", "name": "L/h"},
                {"label": "L/min", "name": "L/min"},
                {"label": "L/s", "name": "L/s"},
                {"label": "ft3/h", "name": "ft3/h"},
                {"label": "ft3/min", "name": "ft3/min"},
                {"label": "ft3/s", "name": "ft3/s"},
                {"label": "gal[US]/h", "name": "gal[US]/h"},
                {"label": "gal[US]/min", "name": "gal[US]/min"},
                {"label": "gal[US]/s", "name": "gal[US]/s"},
                {"label": "gal[UK]/h", "name": "gal[UK]/h"},
                {"label": "gal[UK]/min", "name": "gal[UK]/min"},
                {"label": "gal[UK]/s", "name": "gal[UK]/s"},
            ],
        },
    ],
}
