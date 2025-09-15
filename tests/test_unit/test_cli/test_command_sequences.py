"""
Approval test takes a snapshot of the results and then compare them to last run, ref https://approvaltests.com/,
and fails if they have changed.

If the changes are desired, you can update the snapshot by running `pytest tests/ --force-regen`.
"""

from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk.commands import BuildCommand, CleanCommand, DeployCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, Loader
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils import humanize_collection, iterate_modules
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import COMPLETE_ORG, COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import mock_read_yaml_file

THIS_DIR = Path(__file__).resolve().parent
SNAPSHOTS_DIR = THIS_DIR / "test_build_deploy_snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)
SNAPSHOTS_DIR_CLEAN = THIS_DIR / "test_build_clean_snapshots"
SNAPSHOTS_DIR_CLEAN.mkdir(exist_ok=True)


def find_all_modules() -> Iterator[Path]:
    for module, _ in iterate_modules(BUILTIN_MODULES_PATH):
        if module.name == "references":  # this particular module should never be built or deployed
            continue
        elif module.name == "search":
            # Not ready yet
            continue
        yield pytest.param(module, id=f"{module.parent.name}/{module.name}")


def mock_environments_yaml_file(module_path: Path, monkeypatch: MonkeyPatch) -> None:
    return mock_read_yaml_file(
        {
            "config.dev.yaml": {
                "environment": {
                    "name": "dev",
                    "project": "pytest-project",
                    "type": "dev",
                    "selected": [module_path.name],
                }
            }
        },
        monkeypatch,
        modify=True,
    )


@pytest.mark.parametrize("module_path", list(find_all_modules()))
def test_build_deploy_module(
    module_path: Path,
    build_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    organization_dir: Path,
    data_regression,
) -> None:
    BuildCommand(skip_tracking=True, silent=True).execute(
        verbose=False,
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        selected=[module_path.name],
        build_env_name="dev",
        no_clean=False,
        client=env_vars_with_client.get_client(),
        on_error="raise",
    )

    DeployCommand(skip_tracking=True, silent=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=True,
        dry_run=False,
        include=[],
        verbose=False,
        drop_data=True,
        force_update=False,
    )

    not_mocked = toolkit_client_approval.not_mocked_calls()
    assert not not_mocked, (
        f"The following APIs have been called without being mocked: {not_mocked}, "
        "Please update the list _API_RESOURCES in tests/approval_client.py"
    )

    dump = toolkit_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{module_path.name}.yaml")

    for group_calls in toolkit_client_approval.auth_create_group_calls():
        lost_capabilities = group_calls.capabilities_all_calls - group_calls.last_created_capabilities
        assert not lost_capabilities, (
            f"The group {group_calls.name!r} has lost the capabilities: {', '.join(lost_capabilities)}"
        )


@pytest.mark.parametrize("module_path", list(find_all_modules()))
def test_build_deploy_with_dry_run(
    module_path: Path,
    build_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    organization_dir: Path,
) -> None:
    mock_environments_yaml_file(module_path, monkeypatch)

    BuildCommand(skip_tracking=True, silent=True).execute(
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=env_vars_with_client.get_client(),
        on_error="raise",
        verbose=False,
    )
    DeployCommand(skip_tracking=True, silent=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=True,
        dry_run=True,
        include=[],
        verbose=False,
        drop_data=True,
        force_update=False,
    )

    create_result = toolkit_client_approval.create_calls()
    assert not create_result, f"No resources should be created in dry run: got these calls: {create_result}"
    delete_result = toolkit_client_approval.delete_calls()
    assert not delete_result, f"No resources should be deleted in dry run: got these calls: {delete_result}"


@pytest.mark.parametrize("module_path", list(find_all_modules()))
def test_init_build_clean(
    module_path: Path,
    build_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    organization_dir: Path,
    data_regression,
) -> None:
    mock_environments_yaml_file(module_path, monkeypatch)

    BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        selected=None,
        no_clean=False,
        client=env_vars_with_client.get_client(),
        build_env_name="dev",
        on_error="raise",
    )
    CleanCommand(silent=True, skip_tracking=True).execute(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        dry_run=False,
        include=None,
        verbose=False,
    )

    not_mocked = toolkit_client_approval.not_mocked_calls()
    assert not not_mocked, (
        f"The following APIs have been called without being mocked: {not_mocked}, "
        "Please update the list _API_RESOURCES in tests/approval_client.py"
    )
    dump = toolkit_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR_CLEAN / f"{module_path.name}.yaml")


TEST_CASES = [COMPLETE_ORG]
if Flags.GRAPHQL.is_enabled():
    TEST_CASES.append(COMPLETE_ORG_ALPHA_FLAGS)


@pytest.mark.parametrize("organization_dir", TEST_CASES, ids=[path.name for path in TEST_CASES])
def test_build_deploy_complete_org(
    organization_dir: Path,
    build_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    data_regression,
) -> None:
    BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=env_vars_with_client.get_client(),
        on_error="raise",
    )
    DeployCommand(silent=True, skip_tracking=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=True,
        drop_data=True,
        dry_run=False,
        force_update=False,
        include=None,
        verbose=False,
    )

    not_mocked = toolkit_client_approval.not_mocked_calls()
    assert not not_mocked, (
        f"The following APIs have been called without being mocked: {not_mocked}, "
        "Please update the list _API_RESOURCES in tests/approval_client.py"
    )

    dump = toolkit_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{organization_dir.name}.yaml")

    for group_calls in toolkit_client_approval.auth_create_group_calls():
        lost_capabilities = group_calls.capabilities_all_calls - group_calls.last_created_capabilities
        assert not lost_capabilities, (
            f"The group {group_calls.name!r} has lost the capabilities: {', '.join(lost_capabilities)}"
        )


def test_complete_org_is_complete() -> None:
    modules = ModuleDirectories.load(COMPLETE_ORG)
    used_loader_by_folder_name: dict[str, set[type[Loader]]] = defaultdict(set)

    for module in modules:
        for resource_folder, files in module.source_paths_by_resource_folder.items():
            for loader in CRUDS_BY_FOLDER_NAME[resource_folder]:
                if any(loader.is_supported_file(file) for file in files):
                    used_loader_by_folder_name[resource_folder].add(loader)
    alpha_modules = ModuleDirectories.load(COMPLETE_ORG_ALPHA_FLAGS)
    for module in alpha_modules:
        for resource_folder, files in module.source_paths_by_resource_folder.items():
            for loader in CRUDS_BY_FOLDER_NAME[resource_folder]:
                if any(loader.is_supported_file(file) for file in files):
                    used_loader_by_folder_name[resource_folder].add(loader)

    unused_loaders = {
        loader
        for folder, loaders in CRUDS_BY_FOLDER_NAME.items()
        for loader in loaders
        if loader not in used_loader_by_folder_name[folder]
    }

    # If this assertion fails, it means that the complete_org is not complete.
    # This typically happens when you have just added a new loader and forgotten to add
    # example data for the new resource type in the tests/data/complete_org directory.
    assert not unused_loaders, (
        f"The following {len(unused_loaders)} loaders are not used: {humanize_collection([loader.__name__ for loader in unused_loaders])}"
    )
