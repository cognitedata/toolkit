"""Without the v09 flag, builds keep the legacy insight codes, shapes and display."""

import csv
import io
import json
from io import StringIO
from pathlib import Path

import pytest
from pydantic import ValidationError
from rich.console import Console

from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    ConsistencyError,
    FailedReadYAMLFile,
    InsightList,
    ModelSyntaxWarning,
)
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import SpaceIO
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra
from cognite_toolkit._cdf_tk.yaml_classes import SpaceYAML


@pytest.fixture()
def v09_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    original = FeatureFlag.is_enabled.__wrapped__

    def _is_enabled(flag: Flags) -> bool:
        if flag is Flags.V09:
            return False
        return original(flag)

    monkeypatch.setattr(FeatureFlag, "is_enabled", _is_enabled)


@pytest.mark.usefixtures("v09_disabled")
class TestV09Disabled:
    def test_insights_are_serialized_with_source_files(self, tmp_path: Path) -> None:
        source_file = tmp_path / "modules" / "my_module" / "my.Space.yaml"
        insights = InsightList(
            [ConsistencyError(code="UNKNOWN-REFERENCE", message="msg", source_file=source_file, line=3, column=5)]
        )

        rows = list(csv.DictReader(io.StringIO(insights.to_csv())))
        loaded_json = json.loads(insights.to_json())
        assert list(rows[0]) == ["insight_type", "code", "message", "source_files", "fix", "alpha"]
        assert rows[0]["source_files"] == "modules/my_module/my.Space.yaml"
        assert loaded_json[0]["source_files"] == ["modules/my_module/my.Space.yaml"]
        assert "line" not in loaded_json[0]

        loaded = InsightList.from_csv(insights.to_csv(), tmp_path)
        assert loaded[0].source_file == source_file.resolve()

    def test_read_resource_file_uses_legacy_codes(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()
        invalid_yaml = tmp_path / "resource.Space.yaml"
        invalid_yaml.write_text("key: [unclosed")

        missing = cmd._read_resource_file(tmp_path / "nonexistent.Space.yaml", SpaceIO, [])
        unparsable = cmd._read_resource_file(invalid_yaml, SpaceIO, [])

        assert isinstance(missing, FailedReadYAMLFile) and missing.code == "READ-ERROR"
        assert isinstance(unparsable, FailedReadYAMLFile) and unparsable.code == "YAML-PARSE-ERROR"

    def test_syntax_insights_are_merged_with_legacy_codes(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SpaceYAML.model_validate({"space": "", "extraA": 1, "extraB": 2}, extra="forbid")

        syntax_error, syntax_warnings = BuildV2Command()._create_syntax_insights(
            exc_info.value, tmp_path / "my.Space.yaml", "space: ''\n", SpaceYAML
        )

        assert syntax_error is not None and syntax_error.code == "MODEL-SYNTAX-ERROR"
        assert [warning.code for warning in syntax_warnings] == ["MODEL-SYNTAX-WARNING"]

    def test_failed_read_extra_uses_legacy_code(self, tmp_path: Path) -> None:
        extra = FailedReadExtra(
            source_path=tmp_path, code="REFERENCED-FILE-MISSING", title="Missing file", error="not found"
        )

        assert extra.code == "MISSING"

    def test_displays_legacy_insights(self, tmp_path: Path) -> None:
        output = StringIO()
        insights = InsightList(
            [
                ModelSyntaxWarning(
                    code="MODEL-SYNTAX-WARNING",
                    message="Unknown field: 'Name'",
                    source_file=tmp_path / "modules/my_module/my.Space.yaml",
                )
            ]
        )

        BuildV2Command()._display_insights(
            insights, tmp_path / "build" / "insights.csv", Console(file=output, width=120), verbose=False
        )

        rendered = output.getvalue()
        assert "Model syntax warning in" in rendered
        assert "Unknown field: 'Name'" in rendered
