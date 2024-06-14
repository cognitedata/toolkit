import os
import platform
import subprocess
from collections.abc import Iterable
from pathlib import Path

from constants import PROJECT_INIT_DIR, SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT


def cdf_tk_cmd_all_versions() -> Iterable[tuple[Path, str]]:
    for version in SUPPORTED_TOOLKIT_VERSIONS:
        environment_directory = f".venv{version}"
        if (TEST_DIR_ROOT / environment_directory).exists():
            if platform.system() == "Windows":
                yield Path(f"{environment_directory}/Scripts/"), version
            else:
                yield Path(f"{environment_directory}/bin/"), version
        else:
            raise ValueError(
                f"Environment for version {version} does not exist, run 'create_environments.py' to create it."
            )


def create_init_project():
    modified_env_variables = os.environ.copy()
    repo_root = TEST_DIR_ROOT.parent
    if "PYTHONPATH" in modified_env_variables:
        # Need to remove the repo root from PYTHONPATH to avoid importing the wrong version of the toolkit
        # (This is typically set by the IDE, for example, PyCharm sets it when running tests).
        modified_env_variables["PYTHONPATH"] = modified_env_variables["PYTHONPATH"].replace(str(repo_root), "")

    for old_version_script_dir, version in cdf_tk_cmd_all_versions():
        project_location = PROJECT_INIT_DIR / f"project_{version}"
        if not project_location.exists():
            cmd = [str(old_version_script_dir / "cdf-tk"), "init", str(project_location), "--clean"]
            _ = subprocess.run(cmd, capture_output=True, shell=True, env=modified_env_variables)


if __name__ == "__main__":
    create_init_project()
