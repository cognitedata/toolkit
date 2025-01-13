from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.prototypes.commands.import_ import ImportTransformationCLI
from tests.data import TRANSFORMATION_CLI


class TestImportTransformationCLI:
    @pytest.mark.parametrize("source_name", ["manifest", "manifest_to_asset"])
    def test_import_transformation_cli(self, source_name: str, tmp_path: Path) -> None:
        transformation, schedule, notification, sql = (
            f"{source_name}.Transformation",
            f"{source_name}.Schedule",
            f"{source_name}.Notification",
            f"{source_name}.Transformation.sql",
        )
        expected = {transformation, schedule, notification}
        cmd = ImportTransformationCLI(print_warning=False)
        cmd.execute(TRANSFORMATION_CLI / f"{source_name}.yaml", tmp_path, False, False, False, verbose=False)

        files_by_name = {file.stem: file for file in tmp_path.rglob("*")}
        assert len(files_by_name) == len(expected)
        missing = expected - set(files_by_name)
        assert not missing, f"Missing files: {missing}"
        for name in expected:
            assert (tmp_path / f"{name}.yaml").read_text() == (TRANSFORMATION_CLI / f"{name}.yaml").read_text()
        if (tmp_path / sql).exists():
            assert (tmp_path / sql).read_text() == (TRANSFORMATION_CLI / sql).read_text()
