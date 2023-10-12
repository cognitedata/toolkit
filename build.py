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


def run(build_dir: str) -> None:
    print(
        f"Building config files from templates in ./modules and ./common into {build_dir}..."
    )
    build_config(build_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "build_dir",
        default="./build",
        nargs="?",
        help="Where to write the config files",
    )
    args, unknown_args = parser.parse_known_args()
    run(args.build_dir)
