"""Unit tests for RuleSet and RuleSetVersion CRUDs."""

import tempfile
from pathlib import Path

import pytest
import yaml

from cognite_toolkit._cdf_tk.client.identifiers import RuleSetVersionId
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.cruds._resource_cruds.rulesets import RuleSetVersionIO
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.yaml_classes.ruleset_version import RuleSetVersionYAML

_TURTLE_CONTENT = """@prefix ex: <http://example.org/industrial/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# Define a specific asset
ex:Pump_001
    rdf:type ex:CentrifugalPump ;
    ex:hasName "Main Feed Water Pump" ;
    ex:installedDate "2023-10-12"^^xsd:date ;
    ex:operatingPressure 15.5 ;
    ex:locatedIn ex:Oslo_Facility .

# Define the facility
ex:Oslo_Facility
    rdf:type ex:Site ;
    ex:country "Norway" .
"""


class TestRuleSetVersionCRUDLoadResourceFile:
    def test_load_inline_rules(self) -> None:
        client = ToolkitClientMock()
        crud = RuleSetVersionIO(client, None)

        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = Path(tmp) / "my_rules.RuleSetVersion.yaml"
            yaml_path.write_text(
                yaml.dump(
                    {
                        "rule_set_external_id": "my_rules",
                        "version": "1.0.0",
                        "rules": [_TURTLE_CONTENT],
                    }
                )
            )

            items = crud.load_resource_file(yaml_path)
            assert len(items) == 1
            assert items[0]["rules"] == [_TURTLE_CONTENT]
            assert "rulesFile" not in items[0]

    def test_load_rules_from_ttl_by_convention(self) -> None:
        """When rules is missing, look for .ttl by convention ({stem}.ttl or {rule_set_external_id}.ttl)."""
        client = ToolkitClientMock()
        crud = RuleSetVersionIO(client, None)

        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = Path(tmp) / "my_rules.RuleSetVersion.yaml"
            ttl_path = Path(tmp) / "my_rules.ttl"
            ttl_path.write_text(_TURTLE_CONTENT)

            yaml_path.write_text(
                yaml.dump(
                    {
                        "rule_set_external_id": "my_rules",
                        "version": "1.0.0",
                    }
                )
            )

            items = crud.load_resource_file(yaml_path)
            assert len(items) == 1
            assert items[0]["rules"] == [_TURTLE_CONTENT]

    def test_load_ttl_by_stem_convention(self) -> None:
        """Prefer {stem}.ttl when both conventions match."""
        client = ToolkitClientMock()
        crud = RuleSetVersionIO(client, None)

        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = Path(tmp) / "my_rules.RuleSetVersion.yaml"
            stem_ttl = Path(tmp) / "my_rules.RuleSetVersion.ttl"
            stem_ttl.write_text("from stem")

            yaml_path.write_text(yaml.dump({"rule_set_external_id": "my_rules", "version": "1.0.0"}))

            items = crud.load_resource_file(yaml_path)
            assert items[0]["rules"] == ["from stem"]

    def test_load_no_rules_no_ttl_raises(self) -> None:
        client = ToolkitClientMock()
        crud = RuleSetVersionIO(client, None)

        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = Path(tmp) / "my_rules.RuleSetVersion.yaml"
            yaml_path.write_text(
                yaml.dump(
                    {
                        "rule_set_external_id": "my_rules",
                        "version": "1.0.0",
                    }
                )
            )

            with pytest.raises(ToolkitFileNotFoundError):
                crud.load_resource_file(yaml_path)


class TestRuleSetVersionCRUDSplitResource:
    def test_split_writes_ttl_when_not_exists(self) -> None:
        client = ToolkitClientMock()
        crud = RuleSetVersionIO(client, None)

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "my_rules.RuleSetVersion"
            resource = {
                "rule_set_external_id": "my_rules",
                "version": "1.0.0",
                "rules": [_TURTLE_CONTENT],
            }

            out = list(crud.split_resource(base, resource))
            assert len(out) == 2
            paths = [p for p, _ in out]
            assert base.with_suffix(".ttl") in paths
            assert "rulesFile" not in resource
            assert "rules" not in resource

    def test_split_keeps_inline_when_ttl_exists(self) -> None:
        client = ToolkitClientMock()
        crud = RuleSetVersionIO(client, None)

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "my_rules.RuleSetVersion"
            base.with_suffix(".ttl").write_text("existing")
            resource = {
                "rule_set_external_id": "my_rules",
                "version": "1.0.0",
                "rules": [_TURTLE_CONTENT],
            }

            list(crud.split_resource(base, resource))
            assert resource.get("rules") == [_TURTLE_CONTENT]
            assert "rulesFile" not in resource


class TestRuleSetVersionCRUDGetId:
    def test_get_id_from_dict_camel(self) -> None:
        id_ = RuleSetVersionIO.get_id({"ruleSetExternalId": "my_rules", "version": "1.0.0"})
        assert id_ == RuleSetVersionId(rule_set_external_id="my_rules", version="1.0.0")

    def test_get_id_from_dict_snake(self) -> None:
        id_ = RuleSetVersionIO.get_id({"rule_set_external_id": "my_rules", "version": "1.0.0"})
        assert id_ == RuleSetVersionId(rule_set_external_id="my_rules", version="1.0.0")


class TestRuleSetVersionYAML:
    def test_as_id(self) -> None:
        yaml_obj = RuleSetVersionYAML.model_validate(
            {"ruleSetExternalId": "my_rules", "version": "1.0.0", "rules": ["a"]}
        )
        assert yaml_obj.as_id() == RuleSetVersionId(rule_set_external_id="my_rules", version="1.0.0")
