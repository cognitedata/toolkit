#!/usr/bin/env python
import argparse
import logging

from dotenv import load_dotenv

from scripts.templates import build_config

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(build_dir: str, build_env: str = "dev", clean: bool = False) -> None:
    print(f"Building config files from templates into {build_dir} for environment {build_env}...")

    build_config(dir=build_dir, build_env=build_env, clean=clean)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "build_dir",
        default="./build",
        nargs="?",
        help="Where to write the module files to deploy",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean the build directory before building",
    )
    parser.add_argument(
        "--env",
        action="store",
        nargs="?",
        default="dev",
        help="The environment to build for, defaults to dev",
    )
    args, unknown_args = parser.parse_known_args()
    run(args.build_dir, build_env=args.env, clean=args.clean)
