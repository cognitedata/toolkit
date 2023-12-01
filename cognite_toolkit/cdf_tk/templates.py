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
EXCL_INDEX_SUFFIX = ["sql", "csv", "parquet"]
# Which suffixes to process for template variable replacement
PROC_TMPL_VARS_SUFFIX = ["yaml", "yml", "sql", "csv", "parquet", "json", "txt", "md", "html", "py"]


def read_environ_config(
    root_dir: str = "./",
    build_env: str = "dev",
    tmpl_dirs: [str] = TMPL_DIRS,
    set_env_only: bool = False,
    verbose: bool = False,
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
    if verbose:
        print("  [bold green]INFO:[/] Found defined packages:")
        for name, content in packages.items():
            print(f"    {name}: {content}")
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
                if build_env == "dev" or build_env == "local" or build_env == "demo":
                    print(
                        f"  [bold yellow]WARNING:[/] Project name mismatch (CDF_PROJECT) between local.yaml ({v}) and what is defined in environment ({os.environ.get('CDF_PROJECT','<not_set>')})."
                    )
                    print(f"  Environment is {build_env}, continuing (would have stopped for staging and prod)...")
                else:
                    raise ValueError(
                        f"Project name mismatch (CDF_PROJECT) between local.yaml ({v}) and what is defined in environment ({os.environ['CDF_PROJECT']})."
                    )
        elif k == "type":
            os.environ["CDF_BUILD_TYPE"] = v
        elif k == "deploy":
            print(f"  [bold green]INFO:[/] Building module list for environment {build_env}...")
            for m in v:
                for g2, g3 in packages.items():
                    if m == g2:
                        if verbose:
                            print(f"    Including modules from package {m}: {g3}")
                        for m2 in g3:
                            if m2 not in modules:
                                modules.append(m2)
                    elif m not in modules and packages.get(m) is None:
                        if verbose:
                            print(f"    Including explicitly defined module {m}")
                        modules.append(m)
    if set_env_only:
        return []
    if len(modules) == 0:
        print(
            f"  [bold yellow]WARNING:[/] Found no defined modules in local.yaml, have you configured the environment ({build_env})?"
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


def check_yaml_semantics(parsed: Any, filepath_src: Path, filepath_build: Path, verbose: bool = False) -> bool:
    """Check the yaml file for semantic errors

    parsed: the parsed yaml file
    filepath: the path to the yaml file
    yields: True if the yaml file is semantically acceptable, False if build should fail.
    """
    if parsed is None or filepath_src is None or filepath_build is None:
        return False
    resource_type = filepath_src.parts[-2]
    ext_id = None
    if resource_type == "data_models" and ".space." in filepath_src.name:
        ext_id = parsed.get("space")
        ext_id_type = "space"
    elif resource_type == "data_models" and ".node." in filepath_src.name:
        ext_id = parsed.get("view", {}).get("externalId") or parsed.get("view", {}).get("external_id")
        ext_id_type = "view.externalId"
    elif resource_type == "auth":
        ext_id = parsed.get("name")
        ext_id_type = "name"
    elif resource_type in ["data_sets", "timeseries", "files"] and isinstance(parsed, list):
        ext_id = ""
        ext_id_type = "multiple"
    elif resource_type == "raw":
        ext_id = f"{parsed.get('dbName')}.{parsed.get('tableName')}"
        if "None" in ext_id:
            ext_id = None
        ext_id_type = "dbName and/or tableName"
    else:
        ext_id = parsed.get("externalId") or parsed.get("external_id")
        ext_id_type = "externalId"

    if ext_id is None:
        print(
            f"      [bold yellow]WARNING:[/] the {resource_type} {filepath_src} is missing the {ext_id_type} field(s)."
        )
        return False
    if resource_type == "auth":
        parts = ext_id.split("_")
        if len(parts) < 2:
            if ext_id == "applications-configuration":
                if verbose:
                    print(
                        "      [bold green]INFO:[/] the group applications-configuration does not follow the recommended '_' based namespacing because Infield expects this specific name."
                    )
            else:
                print(
                    f"      [bold yellow]WARNING:[/] the group {filepath_src} has a name [bold]{ext_id}[/] without the recommended '_' based namespacing."
                )
        elif parts[0] != "gp":
            print(
                f"      [bold yellow]WARNING:[/] the group {filepath_src} has a name [bold]{ext_id}[/] without the recommended `gp_` based prefix."
            )
    elif resource_type == "transformations":
        # First try to find the sql file next to the yaml file with the same name
        sql_file1 = filepath_src.parent / f"{filepath_src.stem}.sql"
        if not sql_file1.exists():
            # Next try to find the sql file next to the yaml file with the external_id as filename
            sql_file2 = filepath_src.parent / f"{ext_id}.sql"
            if not sql_file2.exists():
                print("      [bold yellow]WARNING:[/] could not find sql file:")
                print(f"                 [bold]{sql_file1.name}[/] or ")
                print(f"                 [bold]{sql_file2.name}[/]")
                print(f"               Expected to find it next to the yaml file at {sql_file1.parent}.")
                return False
        parts = ext_id.split("_")
        if len(parts) < 2:
            print(
                f"      [bold yellow]WARNING:[/] the transformation {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
            )
        elif parts[0] != "tr":
            print(
                f"      [bold yellow]WARNING:[/] the transformation {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended 'tr_' based prefix."
            )
    elif resource_type == "data_models" and ext_id_type == "space":
        parts = ext_id.split("_")
        if len(parts) < 2:
            print(
                f"      [bold yellow]WARNING:[/] the space {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
            )
        elif parts[0] != "sp":
            if ext_id == "cognite_app_data" or ext_id == "APM_SourceData" or ext_id == "APM_Config":
                if verbose:
                    print(
                        f"      [bold green]INFO:[/] the space {ext_id} does not follow the recommended '_' based namespacing because Infield expects this specific name."
                    )
            else:
                print(
                    f"      [bold yellow]WARNING:[/] the space {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended 'sp_' based prefix."
                )
    elif resource_type == "extraction_pipelines":
        parts = ext_id.split("_")
        if len(parts) < 2:
            print(
                f"      [bold yellow]WARNING:[/] the extraction pipeline {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
            )
        elif parts[0] != "ep":
            print(
                f"      [bold yellow]WARNING:[/] the extraction pipeline {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended 'ep_' based prefix."
            )
    elif resource_type == "data_sets" or resource_type == "timeseries" or resource_type == "files":
        if not isinstance(parsed, list):
            parsed = [parsed]
        for ds in parsed:
            ext_id = ds.get("externalId") or ds.get("external_id")
            if ext_id is None:
                print(
                    f"      [bold yellow]WARNING:[/] the {resource_type} {filepath_src} is missing the {ext_id_type} field."
                )
                return False
            parts = ext_id.split("_")
            # We don't want to throw a warning on entities that should not be governed by the tool
            # in production (i.e. fileseries, files, and other "real" data)
            if resource_type == "data_sets" and len(parts) < 2:
                print(
                    f"      [bold yellow]WARNING:[/] the {resource_type} {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
                )
    return True


def process_config_files(
    dirs: list[str],
    yaml_data: str,
    build_dir: str = "./build",
    build_env: str = "dev",
    clean: bool = False,
    verbose: bool = False,
):
    path = Path(build_dir)
    if path.exists():
        if any(path.iterdir()):
            if clean:
                shutil.rmtree(path)
                path.mkdir()
                print(f"  [bold green]INFO:[/] Cleaned existing build directory {build_dir}.")
            else:
                print("  [bold yellow]WARNING:[/] Build directory is not empty. Use --clean to remove existing files.")
    else:
        path.mkdir()

    local_yaml_path = ""
    yaml_local = {}
    indices = {}
    for directory in dirs:
        if verbose:
            print(f"  [bold green]INFO:[/] Processing module {directory}")
        for dirpath, _, filenames in os.walk(directory):
            # Sort to support 1., 2. etc prefixes
            filenames.sort()
            # When we have traversed out of the module, reset the local yaml config
            if local_yaml_path not in dirpath:
                local_yaml_path == ""
                yaml_local = {}
            for file_name in filenames:
                # Find the root folder and drop processing all files in this dolder
                if file_name == "config.yaml" or file_name == "default.config.yaml":
                    # Pick up this local yaml files
                    local_yaml_path = dirpath
                    yaml_local = read_yaml_files([dirpath])
                    filenames = []
            for file_name in filenames:
                if file_name in EXCL_FILES:
                    continue
                if verbose:
                    print(f"    [bold green]INFO:[/] Processing {file_name}")
                split_path = Path(dirpath).parts
                cdf_path = split_path[len(split_path) - 1]
                new_path = Path(f"{build_dir}/{cdf_path}")
                new_path.mkdir(exist_ok=True, parents=True)
                if (Path(dirpath) / file_name).suffix.lower()[1:] not in PROC_TMPL_VARS_SUFFIX:
                    shutil.copyfile(Path(dirpath) / file_name, new_path / file_name)
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
                orig_file = Path(dirpath) / file_name
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
                    file_name = f"{indices[cdf_path]}.{file_name}"

                filepath = new_path / file_name
                for unmatched in re.findall(pattern=r"\{\{.*?\}\}", string=content):
                    print(
                        f"  [bold yellow]WARNING:[/] Unresolved template variable {unmatched} in {new_path}/{file_name}"
                    )

                filepath.write_text(content)

                if filepath.suffix in {".yaml", ".yml"}:
                    try:
                        parsed = yaml.safe_load(content)
                    except yaml.YAMLError as e:
                        print(
                            f"  [bold red]ERROR:[/] YAML validation error for {file_name} after substituting config variables: \n{e}"
                        )
                        exit(1)
                    if not check_yaml_semantics(
                        parsed=parsed,
                        filepath_src=orig_file,
                        filepath_build=filepath,
                    ):
                        exit(1)


def build_config(
    build_dir: str = "./build",
    source_dir: str = "./",
    build_env: str = "dev",
    clean: bool = False,
    verbose=False,
):
    if build_env is None:
        raise ValueError("build_env must be specified")
    if not source_dir.endswith("/"):
        source_dir = source_dir + "/"
    modules = read_environ_config(
        root_dir=source_dir,
        tmpl_dirs=TMPL_DIRS,
        build_env=build_env,
        verbose=verbose,
    )
    process_config_files(
        dirs=modules,
        yaml_data=read_yaml_files(yaml_dirs=source_dir),
        build_dir=build_dir,
        build_env=build_env,
        clean=clean,
        verbose=verbose,
    )
    # Copy the root deployment yaml files
    shutil.copyfile(Path(source_dir) / "local.yaml", Path(build_dir) / "local.yaml")
    shutil.copyfile(Path(source_dir) / "packages.yaml", Path(build_dir) / "packages.yaml")
    shutil.copyfile(Path(source_dir) / "default.packages.yaml", Path(build_dir) / "default.packages.yaml")
