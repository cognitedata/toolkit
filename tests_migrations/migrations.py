import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from cognite_toolkit._version import __version__


def get_migration(previous_version: str, current_version: str) -> Callable[[Path], None]:
    previous_version = _version_str_to_tuple(previous_version)
    if previous_version <= (0, 1, 0, "b", 6):
        return _migrate_pre_b6_to_b7
    else:
        raise NotImplementedError(f"Migration from {previous_version} to {current_version} is not implemented.")


def _migrate_pre_b6_to_b7(project_path: Path) -> None:
    """Migrate from pre-b6 to b7."""
    default_packages_path = project_path / "cognite_modules" / "default.packages.yaml"
    environments_path = project_path / "environments.yaml"
    config_path = project_path / "config.yaml"
    try:
        default_packages: dict[str, Any] = yaml.safe_load(default_packages_path.read_text())
        environments: dict[str, Any] = yaml.safe_load(environments_path.read_text())
        config_yaml: dict[str, Any] = yaml.safe_load(config_path.read_text())
    except FileNotFoundError:
        raise FileNotFoundError(
            "Could not find one of the required files: default.packages.yaml, environments.yaml, config.yaml"
        )

    # Create _system.yaml
    system_yaml = default_packages.copy()
    system_yaml["cdf_toolkit_version"] = __version__
    (project_path / "cognite_modules" / "_system.yaml").write_text(yaml.dump(system_yaml))

    # Create config.[env].yaml
    for env_name, env_config in environments.items():
        if env_name == "__system":
            continue
        env_config["name"] = env_name
        env_config["selected_modules_and_packages"] = env_config.pop("deploy")
        config_env_yaml = {
            "environment": env_config,
            "modules": config_yaml,
        }
        (project_path / f"config.{env_name}.yaml").write_text(yaml.dump(config_env_yaml))

    # Delete
    default_packages_path.unlink()
    environments_path.unlink()
    config_path.unlink()

    # Delete all default files
    for file in project_path.glob("**/default.*"):
        file.unlink()

    # Added required field 'name' to files
    for file_yaml in project_path.glob("**/*files.yaml"):
        data = yaml.safe_load(file_yaml.read_text().replace("{{", "").replace("}}", ""))
        for entry in data:
            if "name" not in entry:
                entry["name"] = entry["externalId"]
        file_yaml.write_text(yaml.dump(data))


def _version_str_to_tuple(version_str: str) -> tuple[int | str, ...]:
    """Small helper function to convert version strings to tuples.
    >>> _version_str_to_tuple("0.1.0b1")
    (0, 1, 0, 'b', 1)
    >>> _version_str_to_tuple("0.1.0")
    (0, 1, 0)
    >>> _version_str_to_tuple("0.1.0-rc1")
    (0, 1, 0, 'rc', 1)
    """
    version_str = version_str.replace("-", "")
    version_str = re.sub(r"([a-z]+)", r".\1.", version_str)

    return tuple(int(x) if x.isdigit() else x for x in version_str.split("."))
