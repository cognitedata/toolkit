from pathlib import Path

from cognite_toolkit._cdf_tk.commands import ModulesCommand, RepoCommand
from cognite_toolkit._cdf_tk.constants import ROOT_PATH

THIS_FOLDER = Path(__file__).parent.absolute()
DEMO_PROJECT = THIS_FOLDER.parent / "demo_project"

if __name__ == "__main__":
    RepoCommand(skip_tracking=True).init(
        ROOT_PATH,
        verbose=True,
    )

    ModulesCommand(skip_tracking=True).init(DEMO_PROJECT)
