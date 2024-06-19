from pathlib import Path

from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


class ImportTransformationCLI(ToolkitCommand):
    def execute(self, source: Path, destination: Path, overwrite: bool, flatten: bool) -> None:
        print(f"Importing transformation CLI manifests from {source} to {destination}...")
        print(f"Overwrite: {overwrite}")
        print(f"Flatten: {flatten}")
        # Check manifest at:
        # https://cognite-transformations-cli.readthedocs-hosted.com/en/latest/quickstart.html#transformation-manifest
        raise NotImplementedError()
