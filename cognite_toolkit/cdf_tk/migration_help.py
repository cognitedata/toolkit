from pathlib import Path

import yaml
from rich import print

from cognite_toolkit._version import __version__ as current_version
from cognite_toolkit.cdf_tk.utils import read_yaml_file


def print_help(project_dir: Path) -> None:
    cognite_modules = project_dir / "cognite_modules"
    if (cognite_modules / "_system.yaml").exists():
        system_yaml = read_yaml_file(cognite_modules / "_system.yaml")
        try:
            previous_version = system_yaml["cdf_toolkit_version"]
        except KeyError:
            previous_version = None
    elif (project_dir / "environments.yaml").exists():
        environments_yaml = read_yaml_file(project_dir / "environments.yaml")
        try:
            previous_version = environments_yaml["__system__"]["cdf_toolkit_version"]
        except KeyError:
            previous_version = None
    else:
        previous_version = None

    if previous_version is None:
        print(
            "Failed to load previous version, have you changed the "
            "'_system.yaml' or 'environments.yaml' (before 0.1.0b6) file?"
        )
        exit(1)

    if previous_version == current_version:
        print("No changes to the toolkit detected.")
        exit(0)

    _print_difference(project_dir, previous_version)


def _print_difference(project_dir: Path, previous_version: str) -> None:
    if not (migration_file := project_dir / "cognite_modules" / "_migration.yaml").exists():
        print(f"Failed to load migration file '{migration_file}'. Have you deleted it?")
        exit(1)

    migration_yaml = read_yaml_file(migration_file, expected_output="list")
    from_version = next((no for no, entry in enumerate(migration_yaml) if entry["version"] == previous_version), None)
    if from_version is None:
        print(f"Failed to find migration from version '{previous_version}'.")
        exit(1)

    migrations = migration_yaml[: from_version + 1]

    print(f"Migration from version '{previous_version}' to '{current_version}':")
    for migration in migrations:
        # Todo Make a nice readable print tailored to the modules found in the user's project
        to_print = {
            "modules": migration["modules"],
            "tool": migration["tool"],
        }
        print(yaml.safe_dump(to_print))


if __name__ == "__main__":
    _print_difference(Path(__file__).parent.parent, "0.1.0b2")
