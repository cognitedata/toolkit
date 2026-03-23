"""Unit tests for RuleSet and RuleSetVersion resource classes."""

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RuleSetVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset import RuleSetRequest, RuleSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset_version import (
    RuleSetVersionRequest,
    RuleSetVersionResponse,
)

_RULESET = {"externalId": "smoke-test-ruleset", "name": "Smoke Test Rule Set"}

_RULESET_VERSION = {
    "ruleSetExternalId": "smoke-test-ruleset",
    "version": "1.0.0",
    "rules": [
        "@prefix ex: <http://example.org/industrial/> .\n"
        'ex:Pump_001 rdf:type ex:CentrifugalPump ; ex:hasName "Main Feed Water Pump" .',
    ],
}


class TestRuleSetResourceClasses:
    def test_request_round_trip(self) -> None:
        request = RuleSetRequest.model_validate(_RULESET)
        assert request.dump(camel_case=True) == _RULESET

    def test_as_id(self) -> None:
        request = RuleSetRequest.model_validate(_RULESET)
        assert request.as_id() == ExternalId(external_id="smoke-test-ruleset")

    def test_response_to_request(self) -> None:
        response = RuleSetResponse.model_validate({**_RULESET, "createdTime": 1000})
        request = response.as_request_resource()
        assert isinstance(request, RuleSetRequest)
        assert request.external_id == "smoke-test-ruleset"


class TestRuleSetVersionResourceClasses:
    def test_request_round_trip(self) -> None:
        request = RuleSetVersionRequest.model_validate(_RULESET_VERSION)
        dumped = request.dump(camel_case=True)
        # rule_set_external_id is excluded from dump (path param, not body)
        assert dumped["version"] == _RULESET_VERSION["version"]
        assert dumped["rules"] == _RULESET_VERSION["rules"]

    def test_as_id(self) -> None:
        request = RuleSetVersionRequest.model_validate(_RULESET_VERSION)
        assert request.as_id() == RuleSetVersionId(
            rule_set_external_id="smoke-test-ruleset",
            version="1.0.0",
        )

    def test_response_to_request(self) -> None:
        response = RuleSetVersionResponse.model_validate({**_RULESET_VERSION, "createdTime": 1000})
        request = response.as_request_resource()
        assert isinstance(request, RuleSetVersionRequest)
        assert request.rule_set_external_id == "smoke-test-ruleset"
        assert request.version == "1.0.0"
