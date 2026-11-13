from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SpaceId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.data_classes._insights import InsightDefinition
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import AssetIO, LabelIO, ResourceIO, SpaceIO
from cognite_toolkit._cdf_tk.rules._classic import ClassicDeprecationRuleSet

ASSETS = """- externalId: root
  name: Root
- externalId: pump
  name: Pump
  parentExternalId: root
"""


def _resource(path: Path, identifier: ExternalId | SpaceId, crud_cls: type[ResourceIO]) -> BuiltResource:
    return BuiltResource(
        identifier=identifier,
        source_hash="test-hash",
        type=crud_cls.as_resource_type(),
        source_path=path.resolve(),
        build_path=path.resolve(),
        crud_cls=crud_cls,
        has_syntax_error=False,
        module_id=ModuleId(id=Path("modules/my"), path=path.parent.resolve()),
    )


class TestClassicDeprecationRuleSet:
    @pytest.mark.skipif(not FeatureFlag.is_enabled(Flags.ALPHA_RULES), reason="ALPHA_RULES feature flag is not enabled")
    def test_one_recommendation_per_resource_type(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        assets = tmp_path / "my.Asset.yaml"
        assets.write_text(ASSETS)
        labels = tmp_path / "my.Label.yaml"
        labels.write_text("externalId: my_label\nname: My label\n")
        space = tmp_path / "my.Space.yaml"
        space.write_text("space: my_space\n")
        resources = [
            _resource(space, SpaceId(space="my_space"), SpaceIO),
            _resource(assets, ExternalId(external_id="pump"), AssetIO),
            _resource(assets, ExternalId(external_id="root"), AssetIO),
            _resource(labels, ExternalId(external_id="my_label"), LabelIO),
        ]

        results = list(ClassicDeprecationRuleSet(resources=resources).validate())
        insights = [item for item in results if isinstance(item, InsightDefinition)]

        assert [(item.code, item.message, item.source_file, item.line) for item in insights] == [
            (
                "DEPRECATED-FEATURE",
                "Found 2 assets, the first is 'pump'. The Assets API is deprecated and will be retired on "
                "December 31, 2027.",
                assets.resolve(),
                3,
            ),
            (
                "DEPRECATED-FEATURE",
                "Found 1 label, 'my_label'. The Labels API is deprecated and will be retired on December 31, 2027.",
                labels.resolve(),
                1,
            ),
        ]
