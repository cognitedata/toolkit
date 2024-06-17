from constants import PROJECT_INIT_DIR, SUPPORTED_TOOLKIT_VERSIONS

from cognite_toolkit._cdf_tk.utils import calculate_directory_hash


def calculate_hashes():
    exclude_prefixes = set()
    for version in SUPPORTED_TOOLKIT_VERSIONS:
        project_init = PROJECT_INIT_DIR / f"project_{version}"

        version_hash = calculate_directory_hash(project_init / "cognite_modules", exclude_prefixes=exclude_prefixes)
        print(f"Cognite Module Hash for version {version!r}: {version_hash!r}")


if __name__ == "__main__":
    calculate_hashes()
