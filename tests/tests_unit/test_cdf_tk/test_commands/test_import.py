from pathlib import Path

from cognite_toolkit._cdf_tk.prototypes.commands.import_ import ImportTransformationCLI
from tests.tests_unit.data import TRANSFORMATION_CLI


class TestImportTransformationCLI:
    def test_import_transformation_cli(self, tmp_path: Path):
        cmd = ImportTransformationCLI(print_warning=False)
        cmd.execute(TRANSFORMATION_CLI, tmp_path, False, False)
