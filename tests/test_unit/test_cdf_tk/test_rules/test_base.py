from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.rules._base import with_position


@pytest.mark.skipif(not Flags.V09.is_enabled(), reason="V09 feature flag is not enabled")
class TestWithPosition:
    def test_keeps_positions_when_file_has_variables(self, tmp_path: Path) -> None:
        source_file = tmp_path / "my.Container.yaml"
        source_file.write_text("space: {{ space }}\ndescription: {{ text }} is not valid YAML\nproperties:\n  a: b\n")
        insight = BuildWarning(message="m", code="SOME-CODE", source_file=source_file)

        with_position(insight, keys=["properties"])

        assert (insight.line, insight.column) == (3, 1)

    def test_finds_value_built_from_variable(self, tmp_path: Path) -> None:
        source_file = tmp_path / "my.LocationFilter.yaml"
        source_file.write_text("externalId: loc_{{ location }}\ninstanceSpaces:\n  - sp_{{ location }}_files\n")
        insight = BuildWarning(message="m", code="SOME-CODE", source_file=source_file)
        variable = BuildVariable(id=Path("my_module") / "location", value="springfield", is_selected=True)

        with_position(insight, values=["sp_springfield_files"], variables=[variable])

        assert (insight.line, insight.column) == (3, 5)

    def test_no_position_when_field_is_missing(self, tmp_path: Path) -> None:
        source_file = tmp_path / "my.Container.yaml"
        source_file.write_text("space: my_space\n")
        insight = BuildWarning(message="m", code="SOME-CODE", source_file=source_file)

        with_position(insight, keys=["properties"])

        assert (insight.line, insight.column) == (None, None)
