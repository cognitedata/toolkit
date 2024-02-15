import os
import platform
import shutil
import subprocess
from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._version import __version__
from tests_migrations.constants import SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT, chdir
from tests_migrations.migrations import get_migration, modify_environment_to_run_all_modules


def cdf_tk_cmd_all_versions() -> Iterable[tuple[Path, str]]:
    for version in SUPPORTED_TOOLKIT_VERSIONS:
        environment_directory = f".venv{version}"
        if (TEST_DIR_ROOT / environment_directory).exists():
            if platform.system() == "Windows":
                yield pytest.param(Path(f"{environment_directory}/Scripts/"), version, id=f"cdf-tk-{version}")
            else:
                yield pytest.param(Path(f"{environment_directory}/bin/"), version, id=f"cdf-tk-{version}")
        else:
            pytest.skip("Environment for version {version} does not exist, run 'create_environments.py' to create it.")


@pytest.fixture(scope="function")
def local_tmp_project_path() -> Path:
    project_path = TEST_DIR_ROOT / "tmp-project"
    if project_path.exists():
        shutil.rmtree(project_path)
    project_path.mkdir(exist_ok=True)
    yield project_path


@pytest.fixture(scope="function")
def local_build_path() -> Path:
    build_path = TEST_DIR_ROOT / "build"
    if build_path.exists():
        shutil.rmtree(build_path)

    build_path.mkdir(exist_ok=True)
    # This is a small hack to get 0.1.0b1-4 working
    (build_path / "file.txt").touch(exist_ok=True)
    yield build_path


@pytest.mark.parametrize("old_version_script_dir, old_version", list(cdf_tk_cmd_all_versions()))
def tests_init_migrate_build_deploy(
    old_version_script_dir: Path, old_version: str, local_tmp_project_path: Path, local_build_path: Path
) -> None:
    project_name = local_tmp_project_path.name
    build_name = local_build_path.name

    modified_env_variables = os.environ.copy()
    repo_root = TEST_DIR_ROOT.parent
    if "PYTHONPATH" in modified_env_variables:
        # Need to remove the repo root from PYTHONPATH to avoid importing the wrong version of the toolkit
        # (This is typically set by the IDE, for example, PyCharm sets it when running tests).
        modified_env_variables["PYTHONPATH"] = modified_env_variables["PYTHONPATH"].replace(str(repo_root), "")
    previous_version = str(old_version_script_dir / "cdf-tk")

    with chdir(TEST_DIR_ROOT):
        is_upgrade = True
        for cmd in [
            [previous_version, "--version"],
            [previous_version, "init", project_name, "--clean"],
            [previous_version, "build", project_name, "--env", "dev", "--clean"],
            [previous_version, "deploy", "--env", "dev", "--dry-run"],
            # This runs the cdf-tk command from the cognite_toolkit package in the ROOT of the repo.
            ["cdf-tk", "--version"],
            ["cdf-tk", "build", project_name, "--env", "dev", "--build-dir", build_name, "--clean"],
            ["cdf-tk", "deploy", "--env", "dev", "--dry-run"],
        ]:
            if cmd[0] == "cdf-tk" and is_upgrade:
                migration = get_migration(old_version, __version__)
                migration(local_tmp_project_path)
                is_upgrade = False

            if cmd[:2] == [previous_version, "build"]:
                # This is to ensure that we test all modules.
                modify_environment_to_run_all_modules(local_tmp_project_path)

            kwargs = dict(env=modified_env_variables) if cmd[0] == previous_version else dict()
            output = subprocess.run(cmd, capture_output=True, shell=True, **kwargs)

            messaged = output.stderr.decode() or output.stdout.decode()
            assert output.returncode == 0, f"Failed to run {cmd[0]}: {messaged}"

            if cmd[-1] == "--version":
                # This is to check that we use the expected version of the toolkit.
                stdout = output.stdout.decode("utf-8").strip()
                print(f"cmd: {cmd}")
                print(f"output: {output}")
                print(f"stdout: {stdout}")
                expected_version = __version__ if cmd[0] == "cdf-tk" else old_version
                assert stdout.startswith(
                    f"CDF-Toolkit version: {expected_version}"
                ), f"Failed to setup the correct environment for {expected_version}"
