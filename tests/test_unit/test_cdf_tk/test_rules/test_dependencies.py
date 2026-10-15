from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltModule, BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InternalValidatorException
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath, RelativeDirPath
from cognite_toolkit._cdf_tk.resource_ios import DataSetsIO, ResourceIO, ResourceType, TimeSeriesIO
from cognite_toolkit._cdf_tk.rules._dependencies import DependencyRuleSet


def _built_module(yaml_file: Path, crud_cls: type[ResourceIO], identifier: Identifier) -> BuiltModule:
    module_id = ModuleId(id=RelativeDirPath(Path("modules/my")), path=yaml_file.parent.resolve())
    return BuiltModule(
        module_id=module_id,
        yaml_line_count=0,
        resources=[
            BuiltResource(
                identifier=identifier,
                source_hash="test-hash",
                type=ResourceType(resource_folder=crud_cls.folder_name, kind=crud_cls.kind),
                source_path=AbsoluteFilePath(yaml_file.resolve()),
                build_path=AbsoluteFilePath(yaml_file.resolve()),
                crud_cls=crud_cls,
                has_syntax_error=False,
                module_id=module_id,
            )
        ],
    )


class TestDependencyRuleSetCdfApiError:
    def test_retrieve_api_error_is_reported_instead_of_raised(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "my.TimeSeries.yaml"
        yaml_file.write_text("externalId: my_ts\n")
        dataset_id = ExternalId(external_id="my_dataset")
        module = _built_module(yaml_file, TimeSeriesIO, ExternalId(external_id="my_ts"))
        module.resources[0].dependencies.add((DataSetsIO, dataset_id))

        client = MagicMock()
        client.datasets.retrieve.side_effect = ToolkitAPIError("403 Forbidden", code=403)
        rule = DependencyRuleSet(modules=[module], client=client)

        results = list(rule.validate())

        assert [(type(result), result.source, result.message) for result in results] == [
            (
                InternalValidatorException,
                "DataSet",
                "Failed to verify existence of dataset in CDF: 403 Forbidden",
            )
        ]
