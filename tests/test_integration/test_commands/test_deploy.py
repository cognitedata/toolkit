import difflib
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from cognite.client import data_modeling as dm
from mypy.checkexpr import defaultdict
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand, PullCommand
from cognite_toolkit._cdf_tk.data_classes import BuiltModuleList, ResourceDeployResult
from cognite_toolkit._cdf_tk.resources_ios import (
    CRUDS_BY_FOLDER_NAME,
    RESOURCE_CRUD_LIST,
    CogniteFileCRUD,
    FileMetadataCRUD,
    FunctionIO,
    FunctionScheduleIO,
    GraphQLCRUD,
    HostedExtractorDestinationIO,
    HostedExtractorSourceIO,
    ResourceIO,
    ResourceWorker,
    SearchConfigIO,
    StreamlitIO,
    TransformationIO,
    WorkflowTriggerIO,
)
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.data_product import DataProductIO
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.data_product_version import DataProductVersionIO
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.location import LocationFilterIO
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.rulesets import RuleSetIO, RuleSetVersionIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import remove_trailing_newline
from tests import data


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
@pytest.mark.usefixtures("simulator", "simulator_integration", "three_d_file")
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
@pytest.mark.usefixtures("simulator", "simulator_integration", "three_d_file")
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
    # Data Products and Rule Sets APIs are not yet available on the test server.
    # The alpha flag is turned off in cdf.toml so these are not built,
    # but we still exclude the CRUDs to be safe.
    _skip_cruds = {DataProductIO, DataProductVersionIO, RuleSetIO, RuleSetVersionIO}
    with (
        patch.dict(
            os.environ,
            {"EVENTHUB_CLIENT_ID": client_id, "EVENTHUB_CLIENT_SECRET": client_secret},
        ),
        patch.dict(
            "cognite_toolkit._cdf_tk.resources_ios.CRUDS_BY_FOLDER_NAME",
            {f: [c for c in cs if c not in _skip_cruds] for f, cs in CRUDS_BY_FOLDER_NAME.items()},
            clear=True,
        ),
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

    changed_source_files = get_changed_source_files(env_vars, build_dir, built_modules, verbose=True)
    assert not changed_source_files, "Pulling the same source should not change anything"


def create_loader(loader_cls: type[ResourceIO], client: ToolkitClient, build_dir: Path) -> ResourceIO:
    if issubclass(loader_cls, FunctionIO | StreamlitIO):
        # In v0.8, we use filio CRUDs (FileMetadata/CogniteFile) to upload the function/streamlit code.
        # This is the legacy deploy command, which has to do it the old way.
        return loader_cls(client, build_dir, client.console, use_fileio=False)
    elif issubclass(loader_cls, FileMetadataCRUD | CogniteFileCRUD):
        return loader_cls(client, build_dir, client.console, support_upload=False)
    else:
        return loader_cls.create_loader(client, build_dir)


def get_changed_resources(env_vars: EnvironmentVariables, build_dir: Path) -> dict[str, set[Any]]:
    changed_resources: dict[str, set[Any]] = {}
    client = env_vars.get_client()
    print("Looking for changed resources ...")
    for loader_cls in RESOURCE_CRUD_LIST:
        if loader_cls in {HostedExtractorSourceIO, HostedExtractorDestinationIO}:
            # These resources we have no way of knowing if they have changed. So they are always redeployed.
            continue
        if loader_cls in {DataProductIO, DataProductVersionIO, RuleSetIO, RuleSetVersionIO}:
            # Data Products and Rule Sets APIs are not yet available on the test server.
            continue

        loader = create_loader(loader_cls, client, build_dir)

        worker = ResourceWorker(loader, "deploy")
        files = worker.load_files()
        resources = worker.prepare_resources(files, environment_variables=env_vars.dump(), verbose=True)
        if changed := (set(loader.get_ids(resources.to_update)) - {dm.NodeId("sp_nodes", "MyExtendedFile")}):
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
        if (not issubclass(loader_cls, ResourceIO)) or (
            # Authentication that causes the diff to fail
            loader_cls in {HostedExtractorSourceIO, HostedExtractorDestinationIO}
            # External files that cannot (or not yet supported) be pulled
            or loader_cls in {GraphQLCRUD, FunctionIO, StreamlitIO}
            # Have authentication hashes that is different for each environment
            or loader_cls in {TransformationIO, FunctionScheduleIO, WorkflowTriggerIO}
            # LocationFilterLoader needs to split the file into multiple files, so we cannot compare them
            or loader_cls is LocationFilterIO
            # SearchConfigLoader is not supported in pull and post that also will require special handling
            or loader_cls is SearchConfigIO
            # Data Products and Rule Sets APIs are not yet available on the test server.
            or loader_cls in {DataProductIO, DataProductVersionIO, RuleSetIO, RuleSetVersionIO}
        ):
            continue

        loader = create_loader(loader_cls, env_vars.get_client(), build_dir)

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
