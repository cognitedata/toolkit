import re
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import yaml

from cognite_toolkit._version import __version__
from cognite_toolkit.cdf_tk.templates import iterate_modules


def modify_environment_to_run_all_modules(project_path: Path) -> None:
    """Modify the environment to run all modules."""
    environments_path = project_path / "environments.yaml"
    if not environments_path.exists():
        raise FileNotFoundError(f"Could not find environments.yaml in {project_path}")
    environments = yaml.safe_load(environments_path.read_text())

    modules = [module_path.name for module_path, _ in iterate_modules(project_path)]

    for env_name, env_config in environments.items():
        if env_name == "__system":
            continue
        env_config["deploy"] = modules
    environments_path.write_text(yaml.dump(environments))


def get_migration(previous_version: str, current_version: str) -> Callable[[Path], None]:
    previous_version = _version_str_to_tuple(previous_version)
    changes = Changes()
    if previous_version > (0, 1, 0, "b", 6):
        raise NotImplementedError(f"Migration from {previous_version} to {current_version} is not implemented.")

    if previous_version <= (0, 1, 0, "b", 4):
        changes.append(_add_name_to_file_configs)
        changes.append(_add_ignore_null_fields_to_transformation_configs)

    if previous_version <= (0, 1, 0, "b", 6):
        changes.append(_to_config_env_yaml)

    return changes


class Changes:
    def __init__(self) -> None:
        self._changes: list[Callable[[Path], None]] = []

    def append(self, change: Callable[[Path], None]) -> None:
        self._changes.append(change)

    def __call__(self, project_path: Path) -> None:
        for change in self._changes:
            change(project_path)


def _add_name_to_file_configs(project_path: Path) -> None:
    # Added required field 'name' to files
    for file_yaml in _config_yaml_from_folder_name(project_path, "files"):
        if file_yaml.suffix != ".yaml":
            continue
        data = yaml.safe_load(file_yaml.read_text().replace("{{", "").replace("}}", ""))
        for entry in data:
            if "name" not in entry:
                entry["name"] = entry["externalId"]
        file_yaml.write_text(yaml.dump(data))


def _add_ignore_null_fields_to_transformation_configs(project_path: Path) -> None:
    for transformation_yaml in _config_yaml_from_folder_name(project_path, "transformations"):
        if transformation_yaml.suffix != ".yaml" or transformation_yaml.name.endswith(".schedule.yaml"):
            continue
        data = yaml.safe_load(transformation_yaml.read_text().replace("{{", "").replace("}}", ""))
        if isinstance(data, list):
            for entry in data:
                if "ignoreNullFields" not in entry:
                    entry["ignoreNullFields"] = False
        elif isinstance(data, dict):
            if "ignoreNullFields" not in data:
                data["ignoreNullFields"] = False
        transformation_yaml.write_text(yaml.dump(data))


def _to_config_env_yaml(project_path: Path) -> None:
    """Change introduced in b7"""
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


def _config_yaml_from_folder_name(project: Path, folder_name: str) -> Iterable[Path]:
    for module_name, module_files in iterate_modules(project):
        for module_file in module_files:
            if module_file.parent.name == folder_name:
                yield module_file


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
