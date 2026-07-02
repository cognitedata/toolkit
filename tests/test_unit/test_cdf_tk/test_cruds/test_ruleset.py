"""Unit tests for RuleSet and RuleSetVersion CRUDs."""

import gzip
import json
import tempfile
from pathlib import Path

import httpx
import pytest
import respx
import yaml

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.rulesets import RuleSetsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RuleSetVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset_version import RuleSetVersionResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.rulesets import RuleSetVersionIO
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


class TestRuleSetVersionIODumpResource:
    def test_dump_resource_includes_rule_set_external_id_for_deploy_diff(self) -> None:
        """Regression test for CDF-27981: deploy must not delete+recreate unchanged versions."""
        client = ToolkitClientMock()
        io = RuleSetVersionIO(client, None)

        rules = [_TURTLE_CONTENT]
        resource = RuleSetVersionResponse(
            rule_set_external_id="rs_tags",
            version="1.0.0",
            rules=rules,
            created_time=1000,
        )
        local = {
            "ruleSetExternalId": "rs_tags",
            "version": "1.0.0",
            "rules": rules,
        }

        assert io.dump_resource(resource, local) == local


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


class TestRuleSetsAPIRetrieve:
    """CDF-27901: /rulesets/byids does not accept 'ignoreUnknownIds'."""

    def test_retrieve_does_not_send_ignore_unknown_ids(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = RuleSetsAPI(HTTPClient(toolkit_config))
        rule_set = {"externalId": "my_rules", "name": "My Rules", "createdTime": 1000}
        respx_mock.post(toolkit_config.create_api_url("/rulesets/byids")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [rule_set]})
        )

        api.retrieve([ExternalId(external_id="my_rules")])

        assert len(respx_mock.calls) == 1
        body = json.loads(gzip.decompress(respx_mock.calls[0].request.content))
        assert "ignoreUnknownIds" not in body

    def test_ignore_unknown_ids_skips_missing(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        """When ignore_unknown_ids=True, unknown IDs are silently skipped via _request_item_split_retries.

        _request_item_split_retries first tries a batch; on failure it retries each item individually,
        yielding only the successful ones. With 2 items this produces 3 API calls total:
        1 batch (fails) + 2 individual (one succeeds, one fails and is dropped).
        """
        api = RuleSetsAPI(HTTPClient(toolkit_config))
        rule_set = {"externalId": "exists", "name": "Exists", "createdTime": 1000}
        error_body = {"error": {"code": 400, "message": "IDs not found"}}

        def side_effect(request: httpx.Request) -> httpx.Response:
            body = json.loads(gzip.decompress(request.content))
            items = body.get("items", [])
            if any(i.get("externalId") == "missing_one" for i in items):
                return httpx.Response(status_code=400, json=error_body)
            return httpx.Response(status_code=200, json={"items": [rule_set]})

        respx_mock.post(toolkit_config.create_api_url("/rulesets/byids")).mock(side_effect=side_effect)

        result = api.retrieve(
            [ExternalId(external_id="exists"), ExternalId(external_id="missing_one")],
            ignore_unknown_ids=True,
        )

        assert len(result) == 1
        assert result[0].external_id == "exists"
