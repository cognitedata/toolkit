from __future__ import annotations
import os
import shutil
import yaml
import re
from pathlib import Path

# Directory paths for YAML and JSON files
YAML_DIRS = ["./"]
TMPL_DIRS = ["./common", "./modules"]
# Add any other files below that should be included in a build
EXCL_FILES = ["README.md"]
# Which suffixes to exclude when we create indexed files (i.e. they are bundled with their main config file)
EXCL_INDEX_SUFFIX = ["sql"]


def read_module_config(root_dir: str = "./", tmpl_dirs: str = TMPL_DIRS) -> list[str]:
    """Read the global configuration files and return a list of modules in correct order.

    The presence of a module directory in tmpl_dirs is verified.
    Yields:
        List of modules in the order they should be processed.
        Exception(ValueError) if a module is not found in tmpl_dirs.
    """
    global_config = read_yaml_files(root_dir, "global.yaml")
    local_config = read_yaml_files(root_dir, "local.yaml")
    modules = []
    for k, v in local_config.items():
        if k == "deploy":
            for m in v:
                for g2, g3 in global_config.get("packages", {}).items():
                    if m == g2:
                        for m2 in g3:
                            if m2 not in modules:
                                modules.append(m2)
                    else:
                        modules.append(m)

    load_list = []
    module_dirs = {}
    for d in tmpl_dirs:
        if not module_dirs.get(d):
            module_dirs[d] = []
        for dirnames in os.listdir(d):
            module_dirs[d].append(dirnames)
    for m in modules:
        found = False
        for dir, mod in module_dirs.items():
            if m in mod:
                load_list.append(f"{dir}/{m}")
                found = True
                break
        if not found:
            raise ValueError(
                f"Module {m} not found in template directories {tmpl_dirs}."
            )
    return load_list


def read_yaml_files(yaml_dirs, name: str = "config.yaml"):
    """Read all YAML files in the given directories and return a dictionary

    This function will not traverse into sub-directories.

    yaml_dirs: list of directories to read YAML files from
    """

    data = {}
    for directory in yaml_dirs:
        for yaml_file in Path(directory).glob(name):
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
    
    path = Path(build_dir)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()

    local_yaml_path = ""
    yaml_local = {}
    indices = {}
    for directory in dirs:
        for dirpath, _, filenames in os.walk(directory):
            # Sort to support 1., 2. etc prefixes
            filenames.sort()
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
                # For .sql and other dependent files, we do not prefix as we expect them
                # to be named with the external_id of the entitiy they are associated with.
                if file.split(".")[-1] not in EXCL_INDEX_SUFFIX:
                    if not indices.get(cdf_path):
                        indices[cdf_path] = 1
                    else:
                        indices[cdf_path] += 1
                    # Get rid of the local index
                    if re.match("^[0-9]+\\.", file):
                        file = file.split(".", 1)[1]
                    file = f"{indices[cdf_path]}.{file}"
                with open(new_path / file, "w") as f:
                    f.write(content)


def build_config(dir: str = "./build"):
    modules = read_module_config(root_dir="./", tmpl_dirs=TMPL_DIRS)
    process_config_files(
        dirs=modules,
        yaml_data=read_yaml_files(yaml_dirs=YAML_DIRS),
        build_dir=dir,
    )
