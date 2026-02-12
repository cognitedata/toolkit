from pathlib import Path

import pytest
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import RelativeDirPath


@pytest.fixture(scope="session")
def relative_path_adapter() -> TypeAdapter[RelativeDirPath]:
    return TypeAdapter(RelativeDirPath)


class TestRelativeDirPath:
    @pytest.mark.parametrize(
        "input_path, is_relative, error",
        [
            pytest.param("org/modules", False, "is not a relative path", id="Relative path without leading dot"),
            pytest.param("org/modules", True, "", id="Relative path with leading dot"),
            pytest.param("org/file.yaml", True, "is not a directory", id="Path with file suffix"),
        ],
    )
    def test_relative_dir_path(
        self,
        input_path: str,
        is_relative: bool,
        error: str,
        tmp_path: Path,
        relative_path_adapter: TypeAdapter[RelativeDirPath],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        test_path = tmp_path / input_path
        if test_path.suffix:  # If the path has a suffix, create it as a file
            test_path.parent.mkdir(parents=True, exist_ok=True)
            test_path.touch()
        else:
            test_path.mkdir(parents=True, exist_ok=True)
        if is_relative:
            test_path = test_path.relative_to(tmp_path)

        if error:
            with pytest.raises(ValueError) as exc_info:
                relative_path_adapter.validate_python(test_path)
            assert error in str(exc_info.value)
        else:
            result = relative_path_adapter.validate_python(test_path)
            assert result == Path(input_path)
