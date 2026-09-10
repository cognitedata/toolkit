import os
from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RawDatabaseId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline_config import (
    ExtractionPipelineConfigRequest,
    ExtractionPipelineConfigResponse,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.resource_ios import (
    DataSetsIO,
    ExtractionPipelineConfigIO,
    ExtractionPipelineIO,
    RawDatabaseCRUD,
    RawTableCRUD,
    ResourceIO,
)
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, SuccessExtra
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.utils import to_deploy_status


class TestExtractionPipelineDependencies:
    _yaml = """
        externalId: 'ep_src_asset_hamburg_sap'
        name: 'Hamburg SAP'
        dataSetId: 12345
    """

    config_yaml = """
        externalId: 'ep_src_asset'
        description: 'DB extractor config reading data from Springfield SAP'
    """

    def test_load_extraction_pipeline_upsert_update_one(
        self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ) -> None:
        toolkit_client_approval.append(
            ExtractionPipelineConfigResponse,
            ExtractionPipelineConfigResponse(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
                config="\n    logger: \n        {level: WARN}",
                revision=1,
                created_time=0,
            ),
        )

        loader = ExtractionPipelineConfigIO.create_loader(toolkit_client_approval.mock_client)
        assert to_deploy_status(self.config_yaml, loader) == {
            "create": 1,
            "change": 0,
            "delete": 1,
            "unchanged": 0,
        }


class TestExtractionPipelineLoader:
    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "dataSetExternalId": "ds_my_dataset",
                    "rawTables": [
                        {"dbName": "my_db", "tableName": "my_table"},
                        {"dbName": "my_db", "tableName": "my_table2"},
                    ],
                },
                [
                    (DataSetsIO, ExternalId(external_id="ds_my_dataset")),
                    (RawDatabaseCRUD, RawDatabaseId(name="my_db")),
                    (RawTableCRUD, RawTableId(db_name="my_db", name="my_table")),
                    (RawTableCRUD, RawTableId(db_name="my_db", name="my_table2")),
                ],
                id="Extraction pipeline to Table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceIO], Hashable]]) -> None:
        actual = ExtractionPipelineIO.get_dependent_items(item)

        assert list(actual) == expected

    def test_diff_list_contacts_does_not_raise(self, monkeypatch: MonkeyPatch) -> None:
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))
        local = [{"name": "Alice", "email": "alice@example.com", "role": "owner", "sendNotification": True}]
        cdf = [
            {"name": "Alice", "email": "alice@example.com", "role": "owner", "sendNotification": True},
            {"name": "Bob", "email": "bob@example.com", "role": "viewer", "sendNotification": False},
        ]

        local_by_cdf, added = loader.diff_list(local, cdf, ("contacts",))

        assert local_by_cdf == {0: 0}
        assert added == [1]

    @patch.dict(
        os.environ,
        {
            "INGESTION_CLIENT_ID": "this-is-the-ingestion-client-id",
            "INGESTION_CLIENT_SECRET": "this-is-the-ingestion-client-secret",
            "NON-SECRET": "this-is-not-a-secret",
        },
    )
    def test_omit_environment_variables(
        self, env_vars_with_client_cheap: EnvironmentVariables, monkeypatch: MonkeyPatch
    ) -> None:
        local_file = MagicMock(spec=Path)
        local_file.read_text.return_value = """
            - externalId: 'ep_src_asset'
              name: 'Hamburg SAP'
              config: 'secret: ${INGESTION_CLIENT_SECRET}'
            - externalId: 'ep_src_asset_2'
              name: '${NON-SECRET}'
              config: 'secret: ${INGESTION_CLIENT_SECRET}'
        """
        local_file.stem = "ep_src_asset"

        loader = ExtractionPipelineConfigIO.create_loader(env_vars_with_client_cheap.get_client())
        res = loader.load_resource_file(filepath=local_file, environment_variables=env_vars_with_client_cheap.dump())
        # Assert that env vars are skipped for this loader
        assert res[0]["config"] == "secret: ${INGESTION_CLIENT_SECRET}"
        assert res[1]["name"] == "this-is-not-a-secret"


_PIPELINE_YAML = {
    "externalId": "ep_src_asset",
    "name": "Hamburg SAP",
    "dataSetExternalId": "ds_my_dataset",
}


def _write_pipeline_yaml(directory: Path, data: dict, filename: str = "ep_src_asset.ExtractionPipeline.yaml") -> Path:
    yaml_path = directory / filename
    yaml_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return yaml_path


class TestExtractionPipelineDocumentationFile:
    def test_load_documentation_from_documentation_file(self, tmp_path: Path) -> None:
        markdown = "# Hamburg SAP\n\nExtractor documentation.\n"
        (tmp_path / "docs.md").write_text(markdown, encoding="utf-8")
        yaml_path = _write_pipeline_yaml(tmp_path, {**_PIPELINE_YAML, "documentationFile": "docs.md"})
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))

        loaded = loader.load_resource_file(yaml_path)

        assert loaded == [{**_PIPELINE_YAML, "documentation": markdown}]

    def test_load_documentation_from_adjacent_md(self, tmp_path: Path) -> None:
        markdown = "# Adjacent docs\n"
        (tmp_path / "ep_src_asset.md").write_text(markdown, encoding="utf-8")
        yaml_path = _write_pipeline_yaml(tmp_path, _PIPELINE_YAML)
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))

        loaded = loader.load_resource_file(yaml_path)

        assert loaded[0]["documentation"] == markdown

    def test_load_inline_documentation(self, tmp_path: Path) -> None:
        yaml_path = _write_pipeline_yaml(tmp_path, {**_PIPELINE_YAML, "documentation": "Inline docs"})
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))

        loaded = loader.load_resource_file(yaml_path)

        assert loaded[0]["documentation"] == "Inline docs"

    def test_load_ambiguous_documentation_raises(self, tmp_path: Path) -> None:
        (tmp_path / "docs.md").write_text("# Docs\n", encoding="utf-8")
        yaml_path = _write_pipeline_yaml(
            tmp_path, {**_PIPELINE_YAML, "documentation": "Inline", "documentationFile": "docs.md"}
        )
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))

        with pytest.raises(ToolkitYAMLFormatError, match="ambiguously defined"):
            loader.load_resource_file(yaml_path)

    def test_load_missing_documentation_file_raises(self, tmp_path: Path) -> None:
        yaml_path = _write_pipeline_yaml(tmp_path, {**_PIPELINE_YAML, "documentationFile": "missing.md"})
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))

        with pytest.raises(ToolkitFileNotFoundError, match=r"missing.md"):
            loader.load_resource_file(yaml_path)

    def test_load_documentation_file_falls_back_to_adjacent_after_build_rename(self, tmp_path: Path) -> None:
        markdown = "# Built docs\n"
        (tmp_path / "1-ep_src_asset-ep_src_asset.md").write_text(markdown, encoding="utf-8")
        yaml_path = _write_pipeline_yaml(
            tmp_path,
            {**_PIPELINE_YAML, "documentationFile": "original.md"},
            filename="1-ep_src_asset-ep_src_asset.ExtractionPipeline.yaml",
        )
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))

        loaded = loader.load_resource_file(yaml_path)

        assert loaded[0]["documentation"] == markdown

    def test_get_extra_files_from_documentation_file(self, tmp_path: Path) -> None:
        markdown = "# Extra docs\n"
        docs_path = tmp_path / "docs.md"
        docs_path.write_text(markdown, encoding="utf-8")
        yaml_path = _write_pipeline_yaml(tmp_path, {**_PIPELINE_YAML, "documentationFile": "docs.md"})

        extras = list(
            ExtractionPipelineIO.get_extra_files(
                yaml_path, ExternalId(external_id="ep_src_asset"), {"documentationFile": "docs.md"}
            )
        )

        assert len(extras) == 1
        extra = extras[0]
        assert isinstance(extra, SuccessExtra)
        assert extra.source_path == docs_path
        assert extra.suffix == ".md"
        assert extra.content == markdown
        assert extra.description == "extraction pipeline documentation"

    def test_get_extra_files_missing_explicit_file(self, tmp_path: Path) -> None:
        yaml_path = _write_pipeline_yaml(tmp_path, {**_PIPELINE_YAML, "documentationFile": "missing.md"})

        extras = list(
            ExtractionPipelineIO.get_extra_files(
                yaml_path, ExternalId(external_id="ep_src_asset"), {"documentationFile": "missing.md"}
            )
        )

        assert len(extras) == 1
        extra = extras[0]
        assert isinstance(extra, FailedReadExtra)
        assert extra.code == "MISSING"

    def test_get_extra_files_inline_documentation_has_no_extra(self, tmp_path: Path) -> None:
        yaml_path = _write_pipeline_yaml(tmp_path, {**_PIPELINE_YAML, "documentation": "Inline"})

        extras = list(
            ExtractionPipelineIO.get_extra_files(
                yaml_path, ExternalId(external_id="ep_src_asset"), {"documentation": "Inline"}
            )
        )

        assert extras == []

    def test_split_resource_writes_markdown(self, tmp_path: Path) -> None:
        loader = ExtractionPipelineIO(MagicMock(spec=ToolkitClient), None, MagicMock(spec=Console))
        base = tmp_path / "ep_src_asset.ExtractionPipeline.yaml"
        resource = {**_PIPELINE_YAML, "documentation": "# Docs\n"}

        out = list(loader.split_resource(base, resource))

        assert out == [
            (base.with_suffix(".md"), "# Docs\n"),
            (base, _PIPELINE_YAML),
        ]


class TestExtractionPipelineConfigCRUD:
    def test_load_resource_no_warning_on_keyvault(self) -> None:
        resource = {
            "externalId": "ep_src_asset",
            "config": """azure-keyvault:
  authentication-method: client-secret
  keyvault-name: CogniteKeyVault
  tenant-id: ${AZ_ENTRA_TENANT_ID}
  client-id: ${AZ_SERVICE_PRINCIPLE_APPLICATION_ID}
  secret: ${AZ_SERVICE_PRINCIPLE_CLIENT_SECRET}
  password: !keyvault value-secret-name
databases:
-   connection-string:  !keyvault value-secret-name
    name: my_db
    type: odbc""",
        }
        console = MagicMock(spec=Console)
        print_mock = MagicMock()
        console.print = print_mock
        crud = ExtractionPipelineConfigIO(MagicMock(spec=ToolkitClient), None, console=console)

        loaded = crud.load_resource(resource)

        assert isinstance(loaded, ExtractionPipelineConfigRequest)
        # No warning should be printed
        print_mock.assert_not_called()

    def test_load_resource_invalid_yaml_warning(self) -> None:
        resource = {
            "externalId": "ep_src_asset",
            "config": "invalid-yaml: [unclosed_list",
        }
        console = MagicMock(spec=Console)
        print_mock = MagicMock()
        console.print = print_mock
        crud = ExtractionPipelineConfigIO(MagicMock(spec=ToolkitClient), None, console=console)
        loaded = crud.load_resource(resource)

        assert isinstance(loaded, ExtractionPipelineConfigRequest)
        print_mock.assert_called_once()
        args, _ = print_mock.call_args
        _, message = args
        assert "ep_src_asset" in message

    def test_load_resource_yaml_array(self) -> None:
        resource = {
            "externalId": "ep_src_asset",
            "config": "- item1: value1",
        }
        console = MagicMock(spec=Console)
        print_mock = MagicMock()
        console.print = print_mock
        crud = ExtractionPipelineConfigIO(MagicMock(spec=ToolkitClient), None, console=console)
        loaded = crud.load_resource(resource)

        assert isinstance(loaded, ExtractionPipelineConfigRequest)
        print_mock.assert_called_once()
        args, _ = print_mock.call_args
        _, message = args
        assert "ep_src_asset" in message
        assert "a valid YAML mapping" in message
