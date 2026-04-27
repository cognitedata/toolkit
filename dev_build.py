"""Quick dev script: run a local build of complete_org without CDF auth.

Usage:
    uv run python dev_build.py
    uv run python dev_build.py --verbose
"""

import sys
from pathlib import Path

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters

REPO_ROOT = Path(__file__).parent
ORG_DIR = REPO_ROOT / "tests" / "data" / "complete_org"
BUILD_DIR = REPO_ROOT / "build"

params = BuildParameters(
    organization_dir=ORG_DIR,
    build_dir=BUILD_DIR,
    user_selected_modules=["modules/"],
    verbose="--verbose" in sys.argv or "-v" in sys.argv,
)

cmd = BuildV2Command(print_warning=True)
cmd.build(parameters=params, client=None)
