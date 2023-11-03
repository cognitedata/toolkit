#!/usr/bin/env python
import argparse
import difflib
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# from scripts.delete import clean_out_datamodels
from scripts.load import (
    load_app_config,
    load_datamodel,
    load_groups,
    load_raw,
    load_timeseries_metadata,
    load_transformations,
)
from scripts.templates import read_environ_config
from scripts.utils import CDFToolConfig

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using a consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(
    build_dir: str,
    build_env: str = "dev",
    drop: bool = True,
    drop_data: bool = False,
    dry_run: bool = True,
    include: Optional[list[str]] = None,
) -> None:
    # Set environment variables from local.yaml
    read_environ_config(build_env=build_env)
    print(f"Deploying config files from {build_dir} to environment {build_env}...")
    # Configure a client and load credentials from environment
    build_path = Path(__file__).parent / build_dir
    if not build_path.is_dir():
        alternatives = {
            folder.name: f"{folder.parent.name}/{folder.name}"
            for folder in build_path.parent.iterdir()
            if folder.is_dir()
        }
        matches = difflib.get_close_matches(build_path.name, list(alternatives.keys()), n=3, cutoff=0.3)
        print(f"{build_dir} does not exists. Did you mean one of these? {[alternatives[m] for m in matches]}")
        exit(1)
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    print("Using following configurations: ")
    print(ToolGlobals)
    if (include is None or "raw" in include) and Path(f"{build_dir}/raw").is_dir():
        # load_raw() will assume that the RAW database name is set like this in the filename:
        # <index>.<raw_db>.<tablename>.csv
        load_raw(
            ToolGlobals,
            raw_db="default",
            drop=drop,
            file=None,
            dry_run=dry_run,
            directory=f"{build_dir}/raw",
        )
    if ToolGlobals.failed:
        print("Failure to load as expected.")
        exit(1)
    if (include is None or "timeseries" in include) and Path(f"{build_dir}/timeseries").is_dir():
        load_timeseries_metadata(
            ToolGlobals,
            drop=drop,
            file=None,
            dry_run=dry_run,
            directory=f"{build_dir}/timeseries",
        )
    if ToolGlobals.failed:
        print("Failure to load as expected.")
        exit(1)
    if (include is None or "transformations" in include) and Path(f"{build_dir}/transformations").is_dir():
        load_transformations(
            ToolGlobals,
            file=None,
            drop=drop,
            dry_run=dry_run,
            directory=f"{build_dir}/transformations",
        )
    if ToolGlobals.failed:
        print("Failure to load as expected.")
        exit(1)
    if (include is None or "data_models" in include) and (models_dir := Path(f"{build_dir}/data_models")).is_dir():
        # WARNING!!!! The below command will delete EVERYTHING in ALL data models
        # in the project, including instances.
        # clean_out_datamodels(ToolGlobals, dry_run=dry_run, instances=True)
        load_datamodel(
            ToolGlobals,
            drop=drop,
            directory=models_dir,
            delete_containers=drop_data,  # Also delete properties that have been ingested (leaving empty instances)
            delete_spaces=drop_data,  # Also delete spaces if there are no empty instances (needs to be deleted separately)
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print("Failure to load as expected.")
        exit(1)
    if (include is None or "groups" in include) and Path(f"{build_dir}/auth").is_dir():
        load_groups(
            ToolGlobals,
            directory=f"{build_dir}/auth",
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print("Failure to load as expected.")
        exit(1)

    if (include is None or "app_config" in include) and (app_config_dir := Path(f"{build_dir}/app_config")).is_dir():
        load_app_config(
            ToolGlobals,
            directory=app_config_dir,
            dry_run=dry_run,
        )
        if ToolGlobals.failed:
            print("Failure to load as expected.")
            exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "--include",
        help="restrict deploy to: groups,raw,timeseries,transformations,data_models",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        help="whether to do a dry-run, do dry-run if present",
        action="store_true",
    )
    parser.add_argument(
        "--drop",
        help="whether to drop existing configurations, drop per resource if present",
        action="store_true",
    )
    parser.add_argument(
        "--drop-data",
        help="whether to drop existing data, drop data if present (WARNING!! includes data from pipelines)",
        action="store_true",
    )
    parser.add_argument(
        "build_dir",
        default="./build",
        nargs="?",
        help="Where to pick up the config files to deploy",
    )
    parser.add_argument(
        "--env",
        action="store",
        nargs="?",
        default="dev",
        help="The environment to build for, defaults to dev",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Whether to use interactive mode",
        default=False,
    )
    args, unknown_args = parser.parse_known_args()
    if args.include is not None:
        include = args.include.split(",")
    else:
        include = None
    if args.interactive:
        build_modules = list((Path(__file__).parent / "build").iterdir())
        for i, module in enumerate(build_modules):
            print(f"{i}) {module.name}")
        print("a) All")
        print("\nq) Quit")
        answer = input("Select module to deploy: ")
        if answer.casefold() == "a":
            build_dir = "./build"
        elif answer.casefold() == "q":
            exit(0)
        else:
            try:
                build_dir = f"build/{build_modules[int(answer)].name}"
            except ValueError:
                print(f"Invalid selection: {answer}")
                exit(1)
    else:
        build_dir = args.build_dir
    run(
        build_dir=build_dir,
        build_env=args.env,
        dry_run=args.dry_run,
        drop=args.drop,
        drop_data=args.drop_data,
        include=include,
    )
