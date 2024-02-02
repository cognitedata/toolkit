from constants import SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT

from cognite_toolkit.cdf_tk.utils import calculate_directory_hash


def calculate_hashes():
    for version in SUPPORTED_TOOLKIT_VERSIONS:
        cognite_module = (
            TEST_DIR_ROOT / f".venv{version}" / "Lib" / "site-packages" / "cognite_toolkit" / "cognite_modules"
        )
        version_hash = calculate_directory_hash(cognite_module)
        print(f"Cognite Module Hash for version {version!r}: {version_hash!r}")


if __name__ == "__main__":
    calculate_hashes()
