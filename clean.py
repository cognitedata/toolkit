#!/usr/bin/env python
import argparse
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
from scripts.utils import CDFToolConfig
from scripts.delete import (
    delete_raw,
    delete_timeseries,
    delete_datamodel,
    delete_transformations,
)

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(build_dir: str) -> None:
    print(
        f"Cleaning configuration in project based on config files from {build_dir}..."
    )
    # Configure a client and load credentials from environment
    build_path = Path(__file__).parent / build_dir
    if not build_path.is_dir():
        print(f"{build_dir} does not exists.")
        exit(1)
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    print("Using following configurations: ")
    print(ToolGlobals)
    # TODO: #4 Clean up based on configurations in build directory.
    print("TODO: Not yet implemented.")
    print(
        "  The current utils/ delete tooling needs to be adapted to pick up configurations in"
    )
    print("  ./build/ directory.")
    if ToolGlobals.failed:
        print(f"Failure to load as expected.")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "build_dir",
        help="Where to pick up the config files to deploy",
    )
    args, unknown_args = parser.parse_known_args()
    run(args.build_dir)
