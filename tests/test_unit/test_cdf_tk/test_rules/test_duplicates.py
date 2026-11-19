from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import TimeSeriesIO
from cognite_toolkit._cdf_tk.rules._duplicates import DuplicateRuleSet
from cognite_toolkit._cdf_tk.utils.file import relative_to_modules


@pytest.fixture
def alpha_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)


def _built_resource(yaml_file: Path) -> BuiltResource:
    yaml_file.write_text("externalId: my_ts\n")
    module_id = ModuleId(id=Path("modules/my"), path=yaml_file.parent.resolve())
    return BuiltResource(
        identifier=ExternalId(external_id="my_ts"),
        source_hash="test-hash",
        type=TimeSeriesIO.as_resource_type(),
        source_path=yaml_file.resolve(),
        build_path=yaml_file.resolve(),
        crud_cls=TimeSeriesIO,
        has_syntax_error=False,
        module_id=module_id,
    )


class TestDuplicateRuleSet:
    @pytest.mark.usefixtures("alpha_rules")
    def test_duplicated_identifier_is_warned(self, tmp_path: Path) -> None:
        files = [tmp_path / f"{name}.TimeSeries.yaml" for name in ("first", "second", "third")]
        first, second, third = (relative_to_modules(file.resolve()) for file in files)

        results = list(DuplicateRuleSet(resources=[_built_resource(file) for file in files]).validate())

        assert [result.model_dump(include={"code", "message", "source_file", "line"}) for result in results] == [
            {
                "code": "DUPLICATED-IDENTIFIER",
                "message": f"The timeseries 'my_ts' is already defined in {first}, which will be deployed instead. "
                f"It is also defined in {third}.",
                "source_file": second,
                "line": 1,
            },
            {
                "code": "DUPLICATED-IDENTIFIER",
                "message": f"The timeseries 'my_ts' is already defined in {first}, which will be deployed instead. "
                f"It is also defined in {second}.",
                "source_file": third,
                "line": 1,
            },
        ]
