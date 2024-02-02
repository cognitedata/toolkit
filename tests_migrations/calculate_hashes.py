from constants import SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT

from cognite_toolkit.cdf_tk.utils import calculate_directory_hash


def calculate_hashes():
    exclude_prefixes = set()
    for version in SUPPORTED_TOOLKIT_VERSIONS:
        cognite_module = (
            TEST_DIR_ROOT / f".venv{version}" / "Lib" / "site-packages" / "cognite_toolkit" / "cognite_modules"
        )
        if version == "0.1.0b7":
            # From version 0.1.0b7, the default files are no longer copied into the user's project
            exclude_prefixes = {"default."}

        version_hash = calculate_directory_hash(cognite_module, exclude_prefixes=exclude_prefixes)
        print(f"Cognite Module Hash for version {version!r}: {version_hash!r}")


if __name__ == "__main__":
    calculate_hashes()
