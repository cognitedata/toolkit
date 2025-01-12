from __future__ import annotations

import itertools
import re
from collections.abc import Iterator, MutableSequence
from functools import lru_cache
from pathlib import Path

from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print

from cognite_toolkit._cdf_tk.builders import get_loader
from cognite_toolkit._cdf_tk.constants import DOCKER_IMAGE_NAME
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories
from cognite_toolkit._cdf_tk.utils import iterate_modules, read_yaml_file, safe_read, safe_write
from cognite_toolkit._version import __version__


class Change:
    """A change is a single migration step that can be applied to a project."""

    deprecated_from: Version
    required_from: Version | None = None
    has_file_changes: bool = False

    def __init__(self, organization_dir: Path, workflow_dir: Path | None) -> None:
        self._organization_dir = organization_dir
        self._workflow_dir = workflow_dir


class AutomaticChange(Change):
    """An automatic change is a change that can be applied automatically to a project."""

    def do(self) -> set[Path]:
        return set()


class ManualChange(Change):
    """A manual change is a change that requires manual intervention to be applied to a project."""

    def needs_to_change(self) -> set[Path]:
        return set()

    def instructions(self, files: set[Path]) -> str:
        return ""


class SetKindOnFile(AutomaticChange):
    """Adds the kind to the filename of all resource files.

Before `your_file.yaml`:
After `your_file.FileMetadata.yaml`:
    """

    deprecated_from = Version("0.4.0")
    has_file_changes = True

    def do(self) -> set[Path]:
        module_directories = ModuleDirectories.load(self._organization_dir)
        changed: set[Path] = set()
        for module in module_directories:
            for resource_folder, source_files in module.source_paths_by_resource_folder.items():
                for source_file in source_files:
                    if source_file.suffix not in {".yaml", ".yml"}:
                        continue
                    loader, warning = get_loader(source_file, resource_folder, force_pattern=True)
                    if loader is None:
                        print(f"Could not find loader for {source_file}")
                        continue
                    if source_file.stem.casefold().endswith(loader.kind.casefold()):
                        continue
                    new_name = source_file.with_name(f"{source_file.stem}.{loader.kind}{source_file.suffix}")
                    source_file.rename(new_name)
                    changed.add(source_file)
                    for suffix in [".sql", ".csv", ".parquet"]:
                        if (adjacent_file := source_file.with_suffix(suffix)).exists():
                            adjacent_file.rename(new_name.with_suffix(suffix))
                            changed.add(adjacent_file)

        return changed


class FixViewBasedLocationFilter(AutomaticChange):
    """The created view-based location filter has been fixed to be compatible with the CDF API.

Before `your.LocationFilter.yaml`:
```yaml
...
views:
  externalId: my_view
  space: my_space
  version: "1"
  representsEntity: ASSET
...
```
After `your.LocationFilter.yaml`:
```yaml
...
views:
  - externalId: my_view
    space: my_space
    version: "1"
    representsEntity: ASSET
...
    """

    deprecated_from = Version("0.3.1")
    required_from = Version("0.3.1")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for resource_yaml in itertools.chain(self._organization_dir.rglob("*LocationFilter.yaml"), self._organization_dir.rglob("*LocationFilter.yml")):
            content = safe_read(resource_yaml)
            if "views:" not in content:
                continue
            lines = content.splitlines()
            new_lines: list[str] = []
            is_next = False
            indent: int | None = None
            for line in lines:
                if line.startswith("views:"):
                    is_next = True
                elif is_next:
                    indent = len(line) - len(line.lstrip())
                    line = f"{indent * ' '}- {line[indent:]}"
                    is_next = False
                elif indent is not None:
                    line_indent = len(line) - len(line.lstrip())
                    if line_indent == indent:
                        line = f"  {line}"
                    else:
                        indent = None
                new_lines.append(line)
            safe_write(resource_yaml, "\n".join(new_lines))
            changed.add(resource_yaml)
        return changed



class NodeAPICallParametersNoLongerSupported(AutomaticChange):
    """Setting API call parameters in the 'node' section of the config files is no longer supported.

This is now handled correctly by the CDF Toolkit and should be removed from the config files.

For example, in data_models/my_node.node.yaml, before:
```yaml
replace: true
nodes:
 - space: node_space
   externalId: default_infield_config_minimal
```
After:
```yaml
- space: node_space
  externalId: default_infield_config_minimal
```
    """

    deprecated_from = Version("0.3.0b1")
    required_from = Version("0.3.0b1")
    has_file_changes = True

    def do(self) -> set[Path]:
        from cognite_toolkit._cdf_tk.utils import resource_folder_from_path

        api_call_parameters = {
            "skipOnVersionConflict", "replace", "autoCreateDirectRelations"
        }

        changed: set[Path] = set()
        resource_yaml: Path
        for resource_yaml in self._organization_dir.rglob("*.yaml"):
            try:
                resource_folder = resource_folder_from_path(resource_yaml)
            except ValueError:
                continue
            if resource_folder == "data_models" and resource_yaml.stem.casefold().endswith("node"):
                content = safe_read(resource_yaml)
                has_api_call_parameters = False
                new_content:  list[str] = []
                indent: int | None = None
                for line in content.splitlines():
                    if any(line.startswith(parameter) for parameter in api_call_parameters):
                        has_api_call_parameters = True
                        continue
                    if (line.startswith("nodes:") or line.startswith("node:")) and has_api_call_parameters:
                        continue
                    if has_api_call_parameters and indent is None:
                        indent = len(line) - len(line.lstrip())
                    if has_api_call_parameters and indent is not None:
                        line = line[indent:]
                    new_content.append(line)
                if has_api_call_parameters:
                    changed.add(resource_yaml)
                    safe_write(resource_yaml, "\n".join(new_content))

        return changed


class ResourceFolderTimeSeriesDatapointsRemoved(AutomaticChange):
    """The resource folder 'timeseries_datapoints' have been removed.

The `csv` and `parquet` files that were previously stored in the `timeseries_datapoints` folder
should be moved to the `timeseries` folder.

Before:
```bash
    my_module/
       timeseries_datapoints/
          my_datapoints.Datapoints.csv
```
After:
```bash
    my_module/
       timeseries/
          my_datapoints.Datapoints.csv
```
    """

    deprecated_from = Version("0.3.0a7")
    required_from = Version("0.3.0a7")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for module, source_files in iterate_modules(self._organization_dir):
            for resource_dir in module.iterdir():
                if resource_dir.name == "timeseries_datapoints":
                    (module / "timeseries").mkdir(exist_ok=True)
                    for filepath in resource_dir.rglob("*"):
                        target = module / "timeseries" / filepath.relative_to(resource_dir)
                        target.parent.mkdir(exist_ok=True, parents=True)
                        if target.exists():
                            print(f'  [bold red]ERROR ([/][red] Cannot move file [/][bold red]{filepath}[/][red] to [/][bold red]{target}[/][red]): File already exists')
                            continue
                        filepath.rename(target)
                        changed.add(target)
        return changed


class AuthVerifySplit(AutomaticChange):
    """The `cdf auth verify` has been split into `cdf auth init` and `cdf auth verify`.

The `cdf auth init` command initializes the authorization for a user/service principal to run the CDF Toolkit commands,
it will by default also verify the capabilities after the initialization. Thus it replaces the `cdf auth verify` command.
In addition, the `cdf auth verify` command will only verify the capabilities without initializing the authorization.
    """

    deprecated_from = Version("0.3.0a4")
    required_from = Version("0.3.0a4")
    has_file_changes = False


class DeployCleanInteractiveFlagRemoved(AutomaticChange):
    """The `--interactive` flag has been removed from the `cdf deploy` and `cdf clean` commands.
    """

    deprecated_from = Version("0.3.0a4")
    required_from = Version("0.3.0a4")
    has_file_changes = False


class SharedVerboseFlagRemoved(AutomaticChange):
    """The shared `--verbose` flag been removed. Now each command has its own `--verbose` flag.

For example, before:
```bash
    cdf --verbose deploy
```

After:
```bash
    cdf deploy --verbose
```
    """

    deprecated_from = Version("0.3.0a4")
    required_from = Version("0.3.0a4")
    has_file_changes = False


class RenamedOrganizationDirInCDFToml(AutomaticChange):
    """In the cdf.toml file, the 'organization_dir' field in the 'cdf' section has been renamed to
'default_organization_dir'.

In cdf.toml, before:
```toml
[cdf]
organization_dir = "my_organization"
```
After:
```toml
[cdf]
default_organization_dir = "my_organization"
```
"""

    deprecated_from = Version("0.3.0a3")
    required_from = Version("0.3.0a3")
    has_file_changes = True

    def do(self) -> set[Path]:
        cdf_toml = Path.cwd() / "cdf.toml"
        if not cdf_toml.exists():
            return set()
        raw = safe_read(cdf_toml)
        new_cdf_toml = []
        changes: set[Path] = set()
        # We do not parse the TOML file to avoid removing comments
        is_after_cdf_section=False
        for line in raw.splitlines():
            if line.startswith("[cdf]"):
                is_after_cdf_section = True
            if line.startswith("organization_dir = ") and is_after_cdf_section:
                new_line = line.replace("organization_dir", "default_organization_dir")
                new_cdf_toml.append(new_line)
                if new_line != line:
                    changes.add(cdf_toml)
            else:
                new_cdf_toml.append(line)
        cdf_toml.write_text("\n".join(new_cdf_toml), encoding="utf-8")
        return changes


class InitCommandReplaced(AutomaticChange):
    """The `cdf-tk init` has been replaced by `cdf repo init` and `cdf modules init`.

The `cdf repo init` command initializer the current directory with config and git files such as .gitignore.
The `cdf modules init` has an interactive prompt for the user to select the modules to include.
    """

    deprecated_from = Version("0.3.0a1")
    required_from = Version("0.3.0a1")
    has_file_changes = False


class SystemYAMLReplaced(AutomaticChange):
    """The _system.yaml file is now replaced by cdf.toml in the cwd of the project.

Before:
```bash
    my_organization/
        _system.yaml
```
After:
```bash
    cdf.toml
    my_organization/
```
    """

    deprecated_from = Version("0.3.0a1")
    required_from = Version("0.3.0a1")
    has_file_changes = True

    def do(self) -> set[Path]:
        # Avoid circular import
        from .modules import ModulesCommand

        system_yaml = self._organization_dir / "_system.yaml"
        if not system_yaml.exists():
            system_yaml = self._organization_dir / "cognite_modules" / "_system.yaml"
            if not system_yaml.exists():
                return set()
        content = read_yaml_file(system_yaml)
        current_version = content.get("cdf_toolkit_version", __version__)

        cdf_toml_content = ModulesCommand(skip_tracking=True).create_cdf_toml(self._organization_dir)
        cdf_toml_content = cdf_toml_content.replace(f'version = "{__version__}"', f'version = "{current_version}"')

        cdf_toml_path = Path.cwd() / "cdf.toml"
        cdf_toml_path.write_text(cdf_toml_content, encoding="utf-8")
        system_yaml.unlink()
        return {cdf_toml_path, system_yaml}


class ResourceFolderLabelsRenamed(AutomaticChange):
    """The resource folder 'labels' have been renamed to 'classic'.

Before:
```bash
    my_module/
       labels/
          my_labels.Label.yaml
```
After:
```bash
    my_module/
       classic/
          my_labels.Label.yaml
```
    """

    deprecated_from = Version("0.3.0a1")
    required_from = Version("0.3.0a1")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for module, source_files in iterate_modules(self._organization_dir):
            for resource_dir in module.iterdir():
                if resource_dir.name == "labels":
                    (module / "classic").mkdir(exist_ok=True)
                    for files in resource_dir.rglob("*"):
                        target = module / "classic" / files.relative_to(resource_dir)
                        target.parent.mkdir(exist_ok=True, parents=True)
                        files.rename(target)
                        changed.add(target)
        return changed


class RenamedModulesSection(AutomaticChange):
    """The 'modules' section in the config files has been renamed to 'variables'.
This change updates the config files to use the new name.

For example in config.dev.yaml, before:
```yaml
    modules:
      cognite_modules:
        cdf_cluster: ${CDF_CLUSTER}
        cicd_clientId: ${IDP_CLIENT_ID}
        cicd_clientSecret: ${IDP_CLIENT_SECRET}
```
After:
```yaml
    variables:
      cognite_modules:
        cdf_cluster: ${CDF_CLUSTER}
        cicd_clientId: ${IDP_CLIENT_ID}
        cicd_clientSecret: ${IDP_CLIENT_SECRET}
```
    """

    deprecated_from = Version("0.2.0a3")
    required_from = Version("0.2.0a3")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed: set[Path] = set()
        for config_yaml in self._organization_dir.glob("config.*.yaml"):
            data_raw = safe_read(config_yaml)
            # We do not parse the YAML file to avoid removing comments
            updated_file: list[str] = []
            for line in data_raw.splitlines():
                if line.startswith("modules:"):
                    changed.add(config_yaml)
                    updated_file.append(line.replace("modules:", "variables:"))
                else:
                    updated_file.append(line)
            safe_write(config_yaml, "\n".join(updated_file))
        return changed


class BuildCleanFlag(AutomaticChange):
    """The `cdf-tk build` command no longer accepts the `--clean` flag.

The build command now always cleans the build directory before building.
To avoid cleaning the build directory, you can use the `--no-clean` flag.
    """

    deprecated_from = Version("0.2.0a3")
    required_from = Version("0.2.0a3")
    has_file_changes = False


class CommonFunctionCodeNotSupported(ManualChange):
    """."""

    deprecated_from = Version("0.2.0a4")
    required_from = Version("0.2.0a4")
    has_file_changes = True

    def needs_to_change(self) -> set[Path]:
        common_function_code = self._organization_dir / "common_function_code"
        if not common_function_code.exists():
            return set()
        needs_change = {common_function_code}
        for py_file in self._organization_dir.rglob("*.py"):
            content = safe_read(py_file).splitlines()
            use_common_function_code = any(
                (line.startswith("from common") or line.startswith("import common")) for line in content
            )
            if use_common_function_code:
                needs_change.add(py_file)
        return needs_change

    def instructions(self, files: set[Path]) -> str:
        to_update = []
        for module, py_files in itertools.groupby(sorted(files, key=self.get_module_name), key=self.get_module_name):
            if module == Path("."):
                # This is the common_function_code folder
                continue
            to_update.append(f"  - In module {module.relative_to(self._organization_dir).as_posix()!r}:")
            for py_file in py_files:
                to_update.append(f"    - In file {py_file.relative_to(module).as_posix()!r}")
        to_update_str = "\n".join(to_update)
        return (
            "Cognite-Toolkit no longer supports the common functions code.\n"
            f"Please update the following files to not use 'common' module:\n{to_update_str}"
            f"\n\nThen remove the '{self._organization_dir.name}/common_function_code' folder."
        )

    @staticmethod
    @lru_cache(maxsize=128)
    def get_module_name(file_path: Path) -> Path:
        while file_path.parent != file_path:
            if file_path.name == "functions":
                return file_path.parent
            file_path = file_path.parent
        return Path(".")


class FunctionExternalDataSetIdRenamed(AutomaticChange):
    """The 'externalDataSetId' field in function YAML files has been renamed to 'dataSetExternalId'.
This change updates the function YAML files to use the new name.

The motivation for this change is to make the naming consistent with the rest of the Toolkit.

For example, in functions/my_function.yaml, before:
```yaml
externalDataSetId: my_external_id
```
After:
```yaml
dataSetExternalId: my_external_id
```
    """

    deprecated_from = Version("0.2.0a5")
    required_from = Version("0.2.0a5")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed: set[Path] = set()
        for resource_yaml in self._organization_dir.glob("*.yaml"):
            if resource_yaml.parent == "functions":
                content = safe_read(resource_yaml)
                if "externalDataSetId" in content:
                    changed.add(resource_yaml)
                    content = content.replace("externalDataSetId", "dataSetExternalId")
                    safe_write(resource_yaml, content)
        return changed


class ConfigYAMLSelectedRenaming(AutomaticChange):
    """The 'environment.selected_modules_and_packages' field in the config.yaml files has been
renamed to 'selected'.
This change updates the config files to use the new name.

For example, in config.dev.yaml, before:
```yaml
environment:
  selected_modules_and_packages:
    - my_module
```
After:
```yaml
environment:
  selected:
    - my_module
```
    """

    deprecated_from = Version("0.2.0b1")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for config_yaml in self._organization_dir.glob("config.*.yaml"):
            data = safe_read(config_yaml)
            if "selected_modules_and_packages" in data:
                changed.add(config_yaml)
                data = data.replace("selected_modules_and_packages", "selected")
                safe_write(config_yaml, data)
        return changed


class RequiredFunctionLocation(AutomaticChange):
    """Function Resource YAML files are now expected to be in the 'functions' folder.
Before they could be in subfolders inside the 'functions' folder.

This change moves the function YAML files to the 'functions' folder.

For example, before:
```bash
    modules/
      my_module/
          functions/
            some_subdirectory/
                my_function.yaml
```
After:
```bash
    modules/
      my_module/
          functions/
            my_function.yaml
```
    """

    deprecated_from = Version("0.2.0b3")
    required_from = Version("0.2.0b3")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for resource_yaml in self._organization_dir.glob("functions/**/*.yaml"):
            if self._is_function(resource_yaml):
                new_path = self._new_path(resource_yaml)
                if new_path != resource_yaml:
                    resource_yaml.rename(new_path)
                    changed.add(new_path)
        return changed

    @staticmethod
    def _is_function(resource_yaml: Path) -> bool:
        # Functions require a 'name' field and to distinguish from a FunctionSchedule
        # we check that the 'cronExpression' field is not present
        parsed = read_yaml_file(resource_yaml)
        if isinstance(parsed, dict):
            return "name" in parsed and "cronExpression" not in parsed
        elif isinstance(parsed, list):
            return all("name" in item and "cronExpression" not in item for item in parsed)
        return False

    @staticmethod
    def _new_path(resource_yaml: Path) -> Path:
        # Search the path for the 'functions' folder and move the file there
        for parent in resource_yaml.parents:
            if parent.name == "functions":
                return parent / resource_yaml.name
        return resource_yaml


class UpdateModuleVersion(AutomaticChange):
    deprecated_from = parse_version(__version__)
    required_from = parse_version(__version__)
    has_file_changes = True

    def do(self) -> set[Path]:
        cdf_toml = Path.cwd() / "cdf.toml"
        if not cdf_toml.exists():
            return set()
        raw = safe_read(cdf_toml)
        new_cdf_toml = []
        changes: set[Path] = set()
        # We do not parse the TOML file to avoid removing comments
        is_after_module_section=False
        for line in raw.splitlines():
            if line.startswith("[modules]"):
                is_after_module_section = True
            if line.startswith("version = ") and is_after_module_section:
                new_line = f'version = "{__version__}"'
                new_cdf_toml.append(new_line)
                if new_line != line:
                    changes.add(cdf_toml)
            else:
                new_cdf_toml.append(line)
        safe_write(cdf_toml, "\n".join(new_cdf_toml))
        return changes

class UpdateDockerImageVersion(AutomaticChange):
    deprecated_from = parse_version(__version__)
    required_from = parse_version(__version__)
    has_file_changes = True

    def do(self) -> set[Path]:
        if self._workflow_dir is None:
            return set()
        changed = set()
        for workflow_file in itertools.chain(self._workflow_dir.rglob("*.yaml"), self._workflow_dir.rglob("*.yml")):
            content = safe_read(workflow_file)
            new_content = re.sub(rf"image: {DOCKER_IMAGE_NAME}:[0-9.ab]+", f"image: {DOCKER_IMAGE_NAME}:{__version__}", content)
            if new_content != content:
                safe_write(workflow_file, new_content)
                changed.add(workflow_file)
        return changed

UPDATE_MODULE_VERSION_DOCSTRING = """In the cdf.toml file, the 'version' field in the 'module' section has been updated to the same version as the CLI.

This change updated the 'version' field in the cdf.toml file to the same version as the CLI.

In cdf.toml, before:
```toml
[modules]
version = "{module_version}"
```
After:
```toml
[modules]
version = "{cli_version}"
```
    """
UpdateModuleVersion.__doc__ = UPDATE_MODULE_VERSION_DOCSTRING

UPDATE_IMAGE_VERSION_DOCSTRING = """Update the docker image version in the workflow files to the current version of the CLI.

Before:
```yaml
image: cognite/toolkit:{module_version}
```
After:
```yaml
image: cognite/toolkit:{cli_version}
```
    """
UpdateDockerImageVersion.__doc__ = UPDATE_IMAGE_VERSION_DOCSTRING


_CHANGES: list[type[Change]] = [
    change for change in itertools.chain(AutomaticChange.__subclasses__(), ManualChange.__subclasses__())
]


class Changes(list, MutableSequence[Change]):
    @classmethod
    def load(cls, module_version: Version, project_path: Path, workflow_dir: Path | None) -> Changes:
        return cls([change(project_path, workflow_dir) for change in _CHANGES if change.deprecated_from >= module_version])

    @property
    def required_changes(self) -> Changes:
        return Changes([change for change in self if change.required_from is not None])

    @property
    def optional_changes(self) -> Changes:
        return Changes([change for change in self if change.required_from is None])

    def __iter__(self) -> Iterator[Change]:
        return super().__iter__()
