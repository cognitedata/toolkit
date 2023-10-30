#!/usr/bin/env python
import argparse
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from scripts.delete import clean_out_datamodels
from scripts.utils import CDFToolConfig

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(build_dir: str, build_env: str = "dev", dry_run: bool = True, include: Optional[str] = None) -> None:
    print(f"Cleaning configuration in project based on config files from {build_dir}...")
    # Configure a client and load credentials from environment
    build_path = Path(__file__).parent / build_dir
    if not build_path.is_dir():
        print(f"{build_dir} does not exists.")
        exit(1)
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    print("Using following configurations: ")
    print(ToolGlobals)

    if include == "everything":
        clean_out_datamodels(ToolGlobals=ToolGlobals, directory=None, dry_run=dry_run, instances=True)
        return

    if include:
        print(f"Recursively deleting {include}")

        directory = Path.joinpath(build_path, include)
        if not directory.is_dir():
            print(f"{directory} does not exists.")
            exit(1)

        clean_out_datamodels(ToolGlobals=ToolGlobals, directory=directory, dry_run=dry_run)
        return

    # TODO: #4 Clean up based on configurations in build directory.
    print("TODO: Not yet implemented.")
    print("  The current utils/ delete tooling needs to be adapted to pick up configurations in")
    print("  ./build/ directory.")
    if ToolGlobals.failed:
        print("Failure to load as expected.")
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
        "--env", action="store", nargs="?", default="dev", help="The environment to build for, defaults to dev"
    )

    args, unknown_args = parser.parse_known_args()
    run(build_dir=args.build_dir, build_env=args.env, dry_run=args.dry_run, include=args.include)
