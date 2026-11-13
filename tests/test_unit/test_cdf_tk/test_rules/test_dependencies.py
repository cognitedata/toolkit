from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.data_classes._insights import InternalValidatorException
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import DataSetsIO, ResourceIO, ResourceType, TimeSeriesIO
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet
from cognite_toolkit._cdf_tk.utils.file import relative_to_modules


def _built_module(yaml_file: Path, crud_cls: type[ResourceIO], identifier: Identifier) -> BuiltModule:
    module_id = ModuleId(id=Path("modules/my"), path=yaml_file.parent.resolve())
    return BuiltModule(
        module_id=module_id,
        yaml_line_count=0,
        resources=[
            BuiltResource(
                identifier=identifier,
                source_hash="test-hash",
                type=ResourceType(resource_folder=crud_cls.folder_name, kind=crud_cls.kind),
                source_path=yaml_file.resolve(),
                build_path=yaml_file.resolve(),
                crud_cls=crud_cls,
                has_syntax_error=False,
                module_id=module_id,
            )
        ],
    )


@pytest.fixture
def alpha_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)


class TestDependencyRuleSetDuplicates:
    @pytest.mark.usefixtures("alpha_rules")
    def test_duplicated_identifier_is_warned(self, tmp_path: Path) -> None:
        first_file = tmp_path / "first.TimeSeries.yaml"
        second_file = tmp_path / "second.TimeSeries.yaml"
        for yaml_file in (first_file, second_file):
            yaml_file.write_text("externalId: my_ts\n")
        identifier = ExternalId(external_id="my_ts")
        resources = [
            *_built_module(first_file, TimeSeriesIO, identifier).resources,
            *_built_module(second_file, TimeSeriesIO, identifier).resources,
        ]

        results = list(DependencyRuleSet(resources=resources).validate())

        first_path = relative_to_modules(first_file.resolve())
        assert [result.model_dump(include={"code", "message", "source_file", "line"}) for result in results] == [
            {
                "code": "DUPLICATED-IDENTIFIER",
                "message": f"The timeseries 'my_ts' is also defined in {first_path}. "
                f"Only the definition in {first_path} will be deployed.",
                "source_file": relative_to_modules(second_file.resolve()),
                "line": 1,
            }
        ]


class TestDependencyRuleSetCdfApiError:
    def test_retrieve_api_error_is_reported_instead_of_raised(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "my.TimeSeries.yaml"
        yaml_file.write_text("externalId: my_ts\n")
        dataset_id = ExternalId(external_id="my_dataset")
        module = _built_module(yaml_file, TimeSeriesIO, ExternalId(external_id="my_ts"))
        module.resources[0].dependencies.add((DataSetsIO, dataset_id))

        client = MagicMock()
        client.datasets.retrieve.side_effect = ToolkitAPIError("403 Forbidden", code=403)
        rule = DependencyRuleSet(resources=module.resources, client=client)

        results = list(rule.validate())
        assert len(results) == 1
        result = results[0]
        assert isinstance(result, InternalValidatorException)
        assert result.source == "DataSet"
        assert result.message == "Failed to verify existence of dataset in CDF: 403 Forbidden"
