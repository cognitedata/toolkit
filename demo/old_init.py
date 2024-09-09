from pathlib import Path

from cognite_toolkit._cdf_tk.commands import ModulesCommand, RepoCommand

THIS_FOLDER = Path(__file__).parent.absolute()
DEMO_PROJECT = THIS_FOLDER.parent / "demo_project"

if __name__ == "__main__":
    RepoCommand(skip_tracking=True).init(
        THIS_FOLDER.parent,
        verbose=True,
    )

    ModulesCommand(skip_tracking=True).init(DEMO_PROJECT, all=True, clean=True)
