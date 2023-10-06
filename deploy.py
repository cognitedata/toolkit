#!/usr/bin/env python
import argparse
import logging
from dotenv import load_dotenv
from utils.build import build_config

log = logging.getLogger(__name__)

# This is a convenience object that has a CDF client (.client) and
# allows access to environment variables (.environ) using consistent
# naming scheme that is also aligned with recommendations externally.
load_dotenv(".env")


def run(build_dir: str) -> None:
    print(f"Deploying config files from {build_dir}...")
    print("TODO!!!! (simply load_data.py from data-model-examples)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "build_dir",
        help="Where to pick up the config files to deploy",
    )
    args, unknown_args = parser.parse_known_args()
    run(args.build_dir)
