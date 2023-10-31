#!/usr/bin/env python
import argparse
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from scripts.delete import (
    delete_groups,
    delete_raw,
    delete_timeseries,
    delete_transformations,
)
from scripts.load import load_datamodel
from scripts.templates import read_environ_config
from scripts.utils import CDFToolConfig

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(
    build_dir: str,
    build_env: str = "dev",
    dry_run: bool = True,
    include: Optional[str] = None,
) -> None:
    print(f"Cleaning configuration in project based on config files from {build_dir}...")
    # Set environment variables from local.yaml
    read_environ_config(build_env=build_env)
    print(f"Cleaning project from {build_dir} to environment {build_env}...")
    # Configure a client and load credentials from environment
    build_path = Path(__file__).parent / build_dir
    if not build_path.is_dir():
        print(f"{build_dir} does not exists.")
        exit(1)
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    print("Using following configurations: ")
    print(ToolGlobals)
    if (include is None or "raw" in include) and Path(f"{build_dir}/raw").is_dir():
        # load_raw() will assume that the RAW database name is set like this in the filename:
        # <index>.<raw_db>.<tablename>.csv
        delete_raw(
            ToolGlobals,
            raw_db="default",
            dry_run=dry_run,
            directory=f"{build_dir}/raw",
        )
    if ToolGlobals.failed:
        print("Failure to clean raw as expected.")
        exit(1)
    if (include is None or "timeseries" in include) and Path(f"{build_dir}/timeseries").is_dir():
        delete_timeseries(
            ToolGlobals,
            dry_run=dry_run,
            directory=f"{build_dir}/timeseries",
        )
    if ToolGlobals.failed:
        print("Failure to clean timeseries as expected.")
        exit(1)
    if (include is None or "transformations" in include) and Path(f"{build_dir}/transformations").is_dir():
        delete_transformations(
            ToolGlobals,
            dry_run=dry_run,
            directory=f"{build_dir}/transformations",
        )
    if ToolGlobals.failed:
        print("Failure to clean transformations as expected.")
        exit(1)
    if (include is None or "data_models" in include) and (models_dir := Path(f"{build_dir}/data_models")).is_dir():
        # We use the load_datamodel with only_drop=True to ensure that we get a clean
        # deletion of the data model entities and instances.
        load_datamodel(
            ToolGlobals,
            drop=True,
            only_drop=True,
            directory=models_dir,
            delete_removed=True,
            delete_spaces=True,  # Also delete properties that have been ingested (leaving empty instances)
            delete_containers=True,  # Also delete spaces if there are no empty instances (needs to be deleted separately)
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print("Failure to delete data models as expected.")
        exit(1)
    if (include is None or "groups" in include) and Path(f"{build_dir}/auth").is_dir():
        delete_groups(
            ToolGlobals,
            directory=f"{build_dir}/auth",
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print("Failure to clean as expected.")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "build_dir",
        default="./build",
        nargs="?",
        help="Where to pick up the config files to deploy",
    )
    parser.add_argument(
        "--include",
        help="Config to recursively delete",
    )

    parser.add_argument(
        "--dry-run",
        help="whether to do a dry-run, do dry-run if present",
        action="store_true",
    )

    parser.add_argument(
        "--env",
        action="store",
        nargs="?",
        default="dev",
        help="The environment to clean, defaults to dev",
    )

    args, unknown_args = parser.parse_known_args()
    run(
        build_dir=args.build_dir,
        build_env=args.env,
        dry_run=args.dry_run,
        include=args.include,
    )
