from pathlib import Path

from cognite_toolkit._cdf_tk.utils import calculate_directory_hash

TEST_DIR_ROOT = Path(__file__).resolve().parent
PROJECT_INIT_DIR = TEST_DIR_ROOT / "project_inits"

# Todo this file can be deleted when we go to 0.3.0alpha and remove
#   the old manual migration, cognite_toolkit/_cdf_tk/_migration.yaml


def calculate_hashes() -> None:
    for directory in PROJECT_INIT_DIR.iterdir():
        version = directory.name.split("_")[1]
        version_hash = calculate_directory_hash(directory / "cognite_modules")
        print(f"Cognite Module Hash for version {version!r}: {version_hash!r}")


if __name__ == "__main__":
    calculate_hashes()
