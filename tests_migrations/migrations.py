import re
import shutil
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import yaml
from packaging import version

from cognite_toolkit._cdf_tk.utils import iterate_modules
from cognite_toolkit._version import __version__


def modify_environment_to_run_all_modules(project_path: Path) -> None:
    """Modify the environment to run all modules."""
    environments_path = project_path / "environments.yaml"
    if environments_path.exists():
        # This is a older version version
        environments = yaml.safe_load(environments_path.read_text())

        modules = [module_path.name for module_path, _ in iterate_modules(project_path)]

        for env_name, env_config in environments.items():
            if env_name == "__system":
                continue
            env_config["deploy"] = modules
        environments_path.write_text(yaml.dump(environments))
        return
    config_dev_file = project_path / "config.dev.yaml"
    if not config_dev_file.exists():
        raise FileNotFoundError(f"Could not find config.dev.yaml in {project_path}")
    config_dev = yaml.safe_load(config_dev_file.read_text())
    config_dev["environment"]["selected_modules_and_packages"] = [
        # The 'cdf_functions_dummy' module uses the common functions code, which is no longer available
        # so simply skipping it.
        module_path.name
        for module_path, _ in iterate_modules(project_path)
        if module_path.name != "cdf_functions_dummy"
    ]
    config_dev_file.write_text(yaml.dump(config_dev))


def get_migration(previous_version_str: str, current_version: str) -> Callable[[Path], None]:
    previous_version = version.parse(previous_version_str)
    changes = Changes()
    if previous_version < version.parse("0.2.0b5"):
        changes.append(_rename_function_external_dataset_id)
    if previous_version < version.parse("0.2.0b4"):
        changes.append(_move_common_functions_code)
        changes.append(_fix_pump_view_external_id)

    if previous_version < version.parse("0.2.0a3"):
        changes.append(_move_system_yaml_to_root)
        changes.append(_rename_modules_section_to_variables_in_config_yamls)

    if version.parse("0.1.0b7") <= previous_version:
        changes.append(_update_system_yaml)

    if previous_version <= version.parse("0.1.0b4"):
        changes.append(_add_name_to_file_configs)
        changes.append(_add_ignore_null_fields_to_transformation_configs)

    if previous_version <= version.parse("0.1.0b6"):
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


def _rename_function_external_dataset_id(project_path: Path) -> None:
    for resource_yaml in project_path.glob("*.yaml"):
        if resource_yaml.parent == "functions":
            content = resource_yaml.read_text()
            content = content.replace("externalDataSetId", "dataSetExternalId")
            resource_yaml.write_text(content)


def _rename_modules_section_to_variables_in_config_yamls(project_path: Path) -> None:
    for config_yaml in project_path.glob("config.*.yaml"):
        data = yaml.safe_load(config_yaml.read_text())
        if "modules" in data:
            data["variables"] = data.pop("modules")
            config_yaml.write_text(yaml.dump(data))


def _move_common_functions_code(project_path: Path) -> None:
    # It is complex to move the common functions code, so we will just remove
    # the one module that uses it
    cdf_functions_dummy = project_path / "cognite_modules" / "examples" / "cdf_functions_dummy"

    if not cdf_functions_dummy.exists():
        return
    shutil.rmtree(cdf_functions_dummy)


def _fix_pump_view_external_id(project_path: Path) -> None:
    pump_view = (
        project_path
        / "cognite_modules"
        / "experimental"
        / "example_pump_data_model"
        / "data_models"
        / "4.Pump.view.yaml"
    )
    if not pump_view.exists():
        raise FileNotFoundError(f"Could not find Pump.view.yaml in {project_path}")

    pump_view.write_text(pump_view.read_text().replace("external_id", "externalId"))


def _move_system_yaml_to_root(project_path: Path) -> None:
    system_yaml = project_path / "cognite_modules" / "_system.yaml"
    if not system_yaml.exists():
        raise FileNotFoundError(f"Could not find _system.yaml in {project_path}")
    system_yaml.rename(project_path / "_system.yaml")


def _update_system_yaml(project_path: Path) -> None:
    old_system_yaml = project_path / "cognite_modules" / "_system.yaml"
    new_system_yaml = project_path / "_system.yaml"
    if not old_system_yaml.exists() and not new_system_yaml.exists():
        raise FileNotFoundError(f"Could not find _system.yaml in {project_path}")
    system_yaml = old_system_yaml if old_system_yaml.exists() else new_system_yaml
    data = yaml.safe_load(system_yaml.read_text())
    data["cdf_toolkit_version"] = __version__
    system_yaml.write_text(yaml.dump(data))


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
