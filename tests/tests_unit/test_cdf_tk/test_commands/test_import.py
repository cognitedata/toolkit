from pathlib import Path

from cognite_toolkit._cdf_tk.prototypes.commands.import_ import ImportTransformationCLI
from tests.data import TRANSFORMATION_CLI


class TestImportTransformationCLI:
    def test_import_transformation_cli(self, tmp_path: Path) -> None:
        source_name = "manifest"
        transformation, schedule, notification = (
            f"{source_name}.Transformation",
            f"{source_name}.Schedule",
            f"{source_name}.Notification",
        )
        expected = {transformation, schedule, notification}
        cmd = ImportTransformationCLI(print_warning=False)
        cmd.execute(TRANSFORMATION_CLI / f"{source_name}.yaml", tmp_path, False, False, verbose=False)

        files_by_name = {file.stem: file for file in tmp_path.rglob("*")}
        assert len(files_by_name) == len(expected)
        missing = expected - set(files_by_name)
        assert not missing, f"Missing files: {missing}"
        for name in expected:
            assert (tmp_path / f"{name}.yaml").read_text() == (TRANSFORMATION_CLI / f"{name}.yaml").read_text()
