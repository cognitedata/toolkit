from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


class ImportTransformationCLI(ToolkitCommand):
    def execute(self) -> None:
        print("Importing transformation CLI resources.")
