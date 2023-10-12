from __future__ import annotations
import os
import yaml
from typing import Any
from pathlib import Path

# Directory paths for YAML and JSON files
YAML_DIRS = ["./"]
TMPL_DIRS = ["./common", "./modules"]
# Add any other files below that should be included in a build
EXCL_FILES = ["README.md"]


def read_yaml_files(yaml_dirs):
    """Read all YAML files in the given directories and return a dictionary

    This function will not traverse into sub-directories.

    yaml_dirs: list of directories to read YAML files from
    """

    data = {}
    for directory in yaml_dirs:
        for yaml_file in Path(directory).glob("config.yaml"):
            try:
                config_data = yaml.safe_load(yaml_file.read_text())
            except yaml.YAMLError:
                print(f"Error reading {yaml_file}")
                continue
            data.update(config_data)
    # Replace env variables of ${ENV_VAR} with actual value from environment
    for k, v in os.environ.items():
        for k2, v2 in data.items():
            if f"${{{k}}}" in v2:
                if isinstance(data[k2], list):
                    for i in range(len(data[k2])):
                        data[k2][i] = data[k2][i].replace(f"${{{k}}}", v)
                else:
                    data[k2] = data[k2].replace(f"${{{k}}}", v)
    return data



def process_config_files(dirs, yaml_data, build_dir="./build"):
    Path(build_dir).mkdir(exist_ok=True)

    local_yaml_path = ""
    yaml_local = {}
    for directory in dirs:
        for dirpath, _, filenames in os.walk(directory):
            # When we have traversed out of the module, reset the local yaml config
            if local_yaml_path not in dirpath:
                local_yaml_path == ""
                yaml_local = {}
            for file in filenames:
                if file in EXCL_FILES:
                    continue
                # Skip the config.yaml file
                if file == "config.yaml":
                    # Pick up this local yaml files
                    local_yaml_path = dirpath
                    yaml_local = read_yaml_files([dirpath])
                    continue
                with open(dirpath + "/" + file, "rt") as f:
                    content = f.read()
                # Replace the local yaml variables
                for k, v in yaml_local.items():
                    # assuming template variables are in the format {{key}}
                    content = content.replace(f"{{{{{k}}}}}", str(v))
                # Replace the root yaml variables
                for k, v in yaml_data.items():
                    # assuming template variables are in the format {{key}}
                    content = content.replace(f"{{{{{k}}}}}", str(v))

                split_path = dirpath.split("/")
                cdf_path = split_path[len(split_path) - 1]
                new_path = Path(f"{build_dir}/{cdf_path}")
                new_path.mkdir(exist_ok=True, parents=True)
                with open(new_path / file, "w") as f:
                    f.write(content)


def build_config(dir: str = "./build"):
    # TODO #13 Add support for global.yaml and local.yaml configurations of modules and packages to pick up
    process_config_files(
        dirs=TMPL_DIRS,
        yaml_data=read_yaml_files(yaml_dirs=YAML_DIRS),
        build_dir=dir,
    )
