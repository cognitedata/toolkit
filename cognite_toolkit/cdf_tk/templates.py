from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Any

import yaml
from rich import print

TMPL_DIRS = ["common", "modules", "local_modules", "examples", "experimental"]
# Add any other files below that should be included in a build
EXCL_FILES = ["README.md"]
# Which suffixes to exclude when we create indexed files (i.e. they are bundled with their main config file)
EXCL_INDEX_SUFFIX = ["sql"]


def read_environ_config(
    root_dir: str = "./",
    build_env: str = "dev",
    tmpl_dirs: [str] = TMPL_DIRS,
    set_env_only: bool = False,
) -> list[str]:
    """Read the global configuration files and return a list of modules in correct order.

    The presence of a module directory in tmpl_dirs is verified.
    Yields:
        List of modules in the order they should be processed.
        Exception(ValueError) if a module is not found in tmpl_dirs.
    """
    if not root_dir.endswith("/"):
        root_dir = root_dir + "/"
    tmpl_dirs = [root_dir + t for t in tmpl_dirs]
    global_config = read_yaml_files(root_dir, "default.packages.yaml")
    packages = global_config.get("packages", {})
    packages.update(read_yaml_files(root_dir, "packages.yaml").get("packages", {}))
    local_config = read_yaml_files(root_dir, "local.yaml")
    print(f"  Environment is {build_env}, using that section in local.yaml.\n")
    modules = []
    if len(local_config) == 0:
        return []
    try:
        defs = local_config[build_env]
    except KeyError:
        raise ValueError(f"Environment {build_env} not found in local.yaml")

    os.environ["CDF_ENVIRON"] = build_env
    for k, v in defs.items():
        if k == "project":
            if os.environ.get("CDF_PROJECT", "<not set>") != v:
                if build_env == "dev" or build_env == "local":
                    print(
                        f"  [bold red]WARNING:[/] Project name mismatch (CDF_PROJECT) between local.yaml ({v}) and what is defined in environment ({os.environ.get('CDF_PROJECT','<not_set>')})."
                    )
                    print(f"  Environment is {build_env}, continuing (would have stopped for staging and prod)...")
                else:
                    raise ValueError(
                        f"Project name mismatch (CDF_PROJECT) between local.yaml ({v}) and what is defined in environment ({os.environ['CDF_PROJECT']})."
                    )
        elif k == "type":
            os.environ["CDF_BUILD_TYPE"] = v
        elif k == "deploy":
            for m in v:
                for g2, g3 in packages.items():
                    if m == g2:
                        for m2 in g3:
                            if m2 not in modules:
                                modules.append(m2)
                    elif m not in modules and packages.get(m) is None:
                        modules.append(m)
    if set_env_only:
        return []
    if len(modules) == 0:
        print(
            f"  [bold red]WARNING:[/] Found no defined modules in local.yaml, have you configured the environment ({build_env})?"
        )
    load_list = []
    module_dirs = {}
    for d in tmpl_dirs:
        if not module_dirs.get(d):
            module_dirs[d] = []
        try:
            for dirnames in Path(d).iterdir():
                module_dirs[d].append(dirnames.name)
        except Exception:
            ...
    for m in modules:
        found = False
        for dir, mod in module_dirs.items():
            if m in mod:
                load_list.append(f"{dir}/{m}")
                found = True
                break
        if not found:
            raise ValueError(f"Module {m} not found in template directories {tmpl_dirs}.")
    return load_list


def read_yaml_files(
    yaml_dirs: list[str] | str,
    name: str | None = None,
) -> dict[str, Any]:
    """Read all YAML files in the given directories and return a dictionary

    This function will not traverse into sub-directories.

    yaml_dirs: list of directories to read YAML files from
    name: (optional) name of the file(s) to read, either filename or regex. Defaults to config.yaml and default.config.yaml
    """

    if isinstance(yaml_dirs, str):
        yaml_dirs = [yaml_dirs]
    files = []
    if name is None:
        # Order is important!
        for directory in yaml_dirs:
            files.extend(Path(directory).glob("default.config.yaml"))
            files.extend(Path(directory).glob("config.yaml"))
    else:
        name = re.compile(f"^{name}")
        for directory in yaml_dirs:
            for file in Path(directory).glob("*.yaml"):
                if not (name.match(file.name)):
                    continue
                files.append(file)
    data = {}
    for yaml_file in files:
        try:
            config_data = yaml.safe_load(yaml_file.read_text())
        except yaml.YAMLError as e:
            print(f"  [bold red]ERROR:[/] reading {yaml_file}: {e}")
            continue
        data.update(config_data)
    return data


def process_config_files(
    dirs: list[str],
    yaml_data: str,
    build_dir: str = "./build",
    build_env: str = "dev",
    clean: bool = False,
):
    path = Path(build_dir)
    if path.exists():
        if any(path.iterdir()):
            if clean:
                shutil.rmtree(path)
                path.mkdir()
                print(f"  [bold green]INFO:[/] Cleaned existing build directory {build_dir}.")
            else:
                print("  [bold red]WARNING:[/] Build directory is not empty. Use --clean to remove existing files.")
    else:
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
            for file_name in filenames:
                if file_name in EXCL_FILES:
                    continue
                # Skip the config.yaml file
                if file_name == "config.yaml" or file_name == "default.config.yaml":
                    # Pick up this local yaml files
                    local_yaml_path = dirpath
                    yaml_local = read_yaml_files([dirpath])
                    continue
                with open(dirpath + "/" + file_name) as f:
                    content = f.read()
                # Replace the local yaml variables
                for k, v in yaml_local.items():
                    if "." in k:
                        # If the key has a dot, it is a build_env specific variable.
                        # Skip if it's the wrong environment.
                        if k.split(".")[0] != build_env:
                            continue
                        k = k.split(".", 2)[1]
                    # assuming template variables are in the format {{key}}
                    # TODO: issue warning if key is not found, this can indicate a config file error
                    content = content.replace(f"{{{{{k}}}}}", str(v))
                # Replace the root yaml variables
                for k, v in yaml_data.items():
                    if "." in k:
                        # If the key has a dot, it is a build_env specific variable.
                        # Skip if it's the wrong environment.
                        if k.split(".")[0] != build_env:
                            continue
                        k = k.split(".", 2)[1]
                    # assuming template variables are in the format {{key}}
                    content = content.replace(f"{{{{{k}}}}}", str(v))

                split_path = Path(dirpath).parts
                cdf_path = split_path[len(split_path) - 1]
                new_path = Path(f"{build_dir}/{cdf_path}")
                new_path.mkdir(exist_ok=True, parents=True)

                # For .sql and other dependent files, we do not prefix as we expect them
                # to be named with the external_id of the entitiy they are associated with.
                if file_name.split(".")[-1] not in EXCL_INDEX_SUFFIX:
                    if not indices.get(cdf_path):
                        indices[cdf_path] = 1
                    else:
                        indices[cdf_path] += 1
                    # Get rid of the local index
                    if re.match("^[0-9]+\\.", file_name):
                        file_name = file_name.split(".", 1)[1]
                    # If we are processing raw tables, we want to pick up the raw_db config.yaml
                    # variable to determine the database name.
                    if Path(dirpath).name == "raw":
                        file_name = f"{indices[cdf_path]}.{yaml_local.get('raw_db', 'default')}.{file_name}"
                    else:
                        file_name = f"{indices[cdf_path]}.{file_name}"

                filepath = new_path / file_name
                for unmatched in re.findall(pattern=r"\{\{.*?\}\}", string=content):
                    print(f"  [bold red]WARNING:[/] Unresolved template variable {unmatched} in {new_path}/{file_name}")

                filepath.write_text(content)

                if filepath.suffix in {".yaml", ".yml"}:
                    try:
                        yaml.safe_load(content)
                    except yaml.YAMLError as e:
                        print(
                            f"  [bold red]ERROR:[/] YAML validation error for {file_name} after substituting config variables: \n{e}"
                        )
                        exit(1)


def build_config(build_dir: str = "./build", source_dir: str = "./", build_env: str = "dev", clean: bool = False):
    if build_env is None:
        raise ValueError("build_env must be specified")
    if not source_dir.endswith("/"):
        source_dir = source_dir + "/"
    modules = read_environ_config(root_dir=source_dir, tmpl_dirs=TMPL_DIRS, build_env=build_env)
    process_config_files(
        dirs=modules,
        yaml_data=read_yaml_files(yaml_dirs=source_dir),
        build_dir=build_dir,
        build_env=build_env,
        clean=clean,
    )
    # Copy the root deployment yaml files
    shutil.copyfile(Path(source_dir) / "local.yaml", Path(build_dir) / "local.yaml")
    shutil.copyfile(Path(source_dir) / "packages.yaml", Path(build_dir) / "packages.yaml")
    shutil.copyfile(Path(source_dir) / "default.packages.yaml", Path(build_dir) / "default.packages.yaml")
