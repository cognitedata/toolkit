"""Quick dev script: run a local build of complete_org without CDF auth.

Usage:
    uv run python dev_build.py
    uv run python dev_build.py --verbose
    uv run python dev_build.py --verbose -m modules/my_example_module
"""

import argparse
from pathlib import Path

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters

REPO_ROOT = Path(__file__).parent
ORG_DIR = REPO_ROOT / "tests" / "data" / "complete_org"
BUILD_DIR = REPO_ROOT / "build"

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--modules",
    action="append",
    dest="selected_modules",
    help="Specify paths or names to the modules to build",
)
parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose output")
args = parser.parse_args()

params = BuildParameters(
    organization_dir=ORG_DIR,
    build_dir=BUILD_DIR,
    user_selected_modules=args.selected_modules,
    verbose=args.verbose,
)

cmd = BuildV2Command(print_warning=True)
cmd.build(parameters=params, client=None)
