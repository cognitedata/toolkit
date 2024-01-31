import contextlib
import os
import platform
import shutil
import subprocess
from collections.abc import Iterable, Iterator
from pathlib import Path

import pytest

from cognite_toolkit._version import __version__
from tests_migrations.constants import SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT


@contextlib.contextmanager
def chdir(new_dir: Path) -> Iterator[None]:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = Path.cwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)


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
    yield build_path


@pytest.mark.parametrize("old_version_script_dir, old_version", list(cdf_tk_cmd_all_versions()))
def tests_init_migrate_build_deploy(
    old_version_script_dir: Path, old_version: str, local_tmp_project_path: Path, local_build_path: Path
) -> None:
    project_name = local_tmp_project_path.name
    build_name = local_build_path.name

    modified_env_variables = os.environ.copy()
    repo_root = TEST_DIR_ROOT.parent
    # Need to remove the repo root from PYTHONPATH to avoid importing the wrong version of the toolkit
    modified_env_variables["PYTHONPATH"] = modified_env_variables["PYTHONPATH"].replace(str(repo_root), "")
    old_command = str(old_version_script_dir / "cdf-tk")

    with chdir(TEST_DIR_ROOT):
        for cmd in [
            [old_command, "--version"],
            [old_command, "init", project_name, "--clean"],
            [old_command, "build", project_name, "--env", "dev"],
            [old_command, "deploy", "--env", "dev", "--dry-run"],
            # This runs the cdf-tk command from the cognite_toolkit package in the ROOT of the repo.
            ["cdf-tk", "--version"],
            ["cdf-tk", "build", "--env", "dev", "--build-dir", build_name, "--clean"],
            ["cdf-tk", "deploy", "--env", "dev", "--dry-run"],
        ]:
            kwargs = dict(env=modified_env_variables) if cmd[0] == old_command else dict()
            output = subprocess.run(cmd, capture_output=True, shell=True, **kwargs)
            assert output.returncode == 0, f"Failed to run {cmd[0]}: {output.stderr.decode('utf-8')}"

            if cmd[-1] == "--version":
                stdout = output.stdout.decode("utf-8").strip()
                expected_version = __version__ if cmd[0] == "cdf-tk" else old_version
                assert stdout.startswith(
                    f"CDF-Toolkit version: {expected_version}"
                ), f"Failed to setup the correct environment for {expected_version}"
