#!/usr/bin/env python
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.utils import CDFToolConfig
from scripts.load import (
    load_raw,
    load_groups,
    load_timeseries_metadata,
)
from scripts.load import (
    load_datamodel,
    load_transformations_dump,
)

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using a consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(
    build_dir: str, drop: bool = True, dry_run: bool = True, include: list[str] = None
) -> None:
    print(f"Deploying config files from {build_dir}...")
    # Configure a client and load credentials from environment
    build_path = Path(__file__).parent / build_dir
    if not build_path.is_dir():
        print(f"{build_dir} does not exists.")
        exit(1)
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    print("Using following configurations: ")
    print(ToolGlobals)
    if (include is None or "groups" in include) and Path(f"{build_dir}/auth").is_dir():
        load_groups(
            ToolGlobals,
            directory=f"{build_dir}/auth",
            dry_run=dry_run,
        )
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
    if (include is None or "timeseries" in include) and Path(
        f"{build_dir}/timeseries"
    ).is_dir():
        load_timeseries_metadata(
            ToolGlobals,
            drop=drop,
            file=None,
            dry_run=dry_run,
            directory=f"{build_dir}/timeseries",
        )
    if (include is None or "transformations" in include) and Path(
        f"{build_dir}/transformations"
    ).is_dir():
        load_transformations_dump(
            ToolGlobals,
            file=None,
            drop=drop,
            dry_run=dry_run,
            directory=f"{build_dir}/transformations",
        )
    if (include is None or "source_models" in include) and (
        models_dir := Path(f"{build_dir}/source_models")
    ).is_dir():
        load_datamodel(
            ToolGlobals,
            drop=drop,
            directory=models_dir,
            dry_run=dry_run,
        )
    if (include is None or "domain_models" in include) and (
        models_dir := Path(f"{build_dir}/domain_models")
    ).is_dir():
        load_datamodel(
            ToolGlobals,
            drop=drop,
            directory=models_dir,
            dry_run=dry_run,
        )
    if (include is None or "solution_models" in include) and (
        models_dir := Path(f"{build_dir}/solution_models")
    ).is_dir():
        load_datamodel(
            ToolGlobals,
            drop=drop,
            directory=models_dir,
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print(f"Failure to load as expected.")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "--include",
        help="restrict deploy to: groups,raw,timeseries,transformations,source_models,domain_models,solution_models",
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
        help="whether to drop existing data, drop data if present",
        action="store_true",
    )
    parser.add_argument(
        "build_dir",
        default="./build",
        nargs="?",
        help="Where to pick up the config files to deploy",
    )
    args, unknown_args = parser.parse_known_args()
    if args.include is not None:
        include = args.include.split(",")
    else:
        include = None
    run(
        build_dir=args.build_dir,
        dry_run=args.dry_run,
        drop=args.drop,
        include=include,
    )
