#!/usr/bin/env python
import argparse
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
from scripts.utils import CDFToolConfig
from scripts.load import (
    load_raw,
    load_readwrite_group,
    load_timeseries_metadata,
)
from scripts.datamodel import load_datamodel_dump
from scripts.transformations import load_transformations_dump

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using a consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(build_dir: str) -> None:
    print(f"Deploying config files from {build_dir}...")
    # Configure a client and load credentials from environment
    build_path = Path(__file__).parent / build_dir
    if not build_path.is_dir():
        print(f"{build_dir} does not exists.")
        exit(1)
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    # TODO: #14 This is confusing heritage from data-model-examples. Refactor to use config.yaml and module structure.
    ToolGlobals.example = "default"
    print("Using following configurations: ")
    print(ToolGlobals)
    # TODO: #6 This is a very limited support. Needs to be expanded to support configurable groups.
    if Path(f"{build_dir}/auth").is_dir():
        capabilities = json.loads(
            (build_path / "auth/readwrite.capabilities.json").read_text()
        )
        load_readwrite_group(
            ToolGlobals, capabilities=capabilities, source_id="readwrite"
        )
    if Path(f"{build_dir}/raw").is_dir():
        # TODO: #7 load_raw only loads one database as configured in ToolGlobals.config, needs more dynamic support
        load_raw(ToolGlobals, drop=True, file=None, directory=f"f{build_dir}/raw")
    if Path(f"{build_dir}/timeseries").is_dir():
        load_timeseries_metadata(
            ToolGlobals, drop=True, file=None, directory=f"f{build_dir}/timeseries"
        )
    if Path(f"{build_dir}/transformations").is_dir():
        load_transformations_dump(
            ToolGlobals, file=None, drop=True, directory=f"{build_dir}/transformations"
        )
    if (models_dir := Path(f"{build_dir}/domain_models")).is_dir():
        load_datamodel_dump(
            ToolGlobals, drop=True, directory=models_dir, dry_run=True
        )
    if (models_dir := Path(f"{build_dir}/solution_models")).is_dir():
        load_datamodel_dump(
            ToolGlobals, drop=True, directory=models_dir, dry_run=True
        )
    if ToolGlobals.failed:
        print(f"Failure to load as expected.")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "build_dir",
        default="./build",
        nargs="?",
        help="Where to pick up the config files to deploy",
    )
    args, unknown_args = parser.parse_known_args()
    run(args.build_dir)
