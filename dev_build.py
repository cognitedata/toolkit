"""Quick dev script: run a local build of complete_org without CDF auth.

Usage:
    uv run python dev_build.py
    uv run python dev_build.py --verbose
    uv run python dev_build.py --verbose -m modules/my_example_module
    uv run python dev_build.py --verbose --org-dir /tmp/toolkit-build-error-sims/misplaced_org -m modules/
"""

import argparse
from pathlib import Path

from rich import print

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.exceptions import ToolkitError

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
parser.add_argument("--org-dir", type=Path, default=ORG_DIR, help="Where to find the organization directory")
parser.add_argument("--build-dir", type=Path, default=BUILD_DIR, help="Where to save the built module files")
parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose output")
args = parser.parse_args()

params = BuildParameters(
    organization_dir=args.org_dir,
    build_dir=args.build_dir,
    user_selected_modules=args.selected_modules,
    verbose=args.verbose,
)

cmd = BuildV2Command(print_warning=True)
try:
    cmd.run(lambda: cmd.build(parameters=params, client=None))
except ToolkitError as err:
    print(f"  [bold red]ERROR ([/][red]{type(err).__name__}[/][bold red]):[/] {err}")
    raise SystemExit(1) from err
