import shutil
from pathlib import Path

from cognite_toolkit._cdf_tk.constants import ROOT_PATH
from cognite_toolkit._cdf_tk.data_classes import CDFToml, ModuleDirectories

THIS_FOLDER = Path(__file__).parent.absolute()
DEMO_PROJECT = THIS_FOLDER.parent / "demo_project"

if __name__ == "__main__":
    modules_directories = ModuleDirectories.load(ROOT_PATH, {Path("")})
    DEMO_PROJECT.mkdir(exist_ok=True, parents=True)

    modules_directories.dump(DEMO_PROJECT)

    for file_name in [
        "README.md",
        ".gitignore",
        ".env.tmpl",
    ]:
        shutil.copy(ROOT_PATH / file_name, DEMO_PROJECT / file_name)

    shutil.copy(ROOT_PATH / CDFToml.file_name_tmpl, DEMO_PROJECT.parent / CDFToml.file_name_tmpl)
