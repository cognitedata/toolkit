import gzip
import json

import httpx2
import pytest
import respx
from pydantic import TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.domains import DomainsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.domain import DomainRequest, DomainResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, DataDomainsAcl, DataDomainScope
from cognite_toolkit._cdf_tk.resource_ios import DomainIO, GroupIO, TransformationIO, WorkflowIO
from cognite_toolkit._cdf_tk.yaml_classes import DomainYAML, GroupYAML, TransformationYAML, WorkflowYAML


class TestDomainYAML:
    def test_valid_domain(self) -> None:
        domain = DomainYAML.model_validate(
            {
                "externalId": "my_domain",
                "name": "My Domain",
                "description": "Domain description",
            }
        )
        assert domain.as_id() == ExternalId(external_id="my_domain")

    @pytest.mark.parametrize("invalid_id", ["UNGOVERNED", "ungoverned"])
    def test_rejects_ungoverned_external_id(self, invalid_id: str) -> None:
        with pytest.raises(ValidationError, match="UNGOVERNED"):
            DomainYAML.model_validate({"externalId": invalid_id, "name": "Invalid Domain"})


class TestDomainIO:
    def test_minimum_scope_and_acl(self) -> None:
        item = DomainRequest(external_id="my_domain", name="My Domain")
        scope = DomainIO.get_minimum_scope([item])
        assert scope == DataDomainScope(external_ids=["my_domain"])

        acls = list(DomainIO.create_acl({"READ", "WRITE"}, scope))
        assert acls == [
            DataDomainsAcl(
                actions=["READ", "CREATE", "UPDATE", "DELETE"],
                scope=DataDomainScope(external_ids=["my_domain"]),
            )
        ]

        all_acls = list(DomainIO.create_acl({"READ"}, AllScope()))
        assert all_acls == [DataDomainsAcl(actions=["READ"], scope=AllScope())]

    def test_dump_resource_omits_unset_description(self, toolkit_client_with_lookup: ToolkitClient) -> None:
        io = DomainIO.create_io(toolkit_client_with_lookup)
        response = DomainResponse(external_id="my_domain", name="My Domain", description=None)
        dumped = io.dump_resource(response, local={"externalId": "my_domain", "name": "My Domain"})
        assert "description" not in dumped

    def test_dependencies_from_group_and_resources(self) -> None:
        group = TypeAdapter(GroupYAML).validate_python(
            {
                "name": "domain-group",
                "sourceId": "123",
                "capabilities": [
                    {
                        "dataDomainsAcl": {
                            "actions": ["READ"],
                            "scope": {"dataDomainScope": {"externalIds": ["my_domain", "UNGOVERNED"]}},
                        }
                    },
                    {
                        "transformationsAcl": {
                            "actions": ["READ", "WRITE"],
                            "scope": {"dataDomainScope": {"externalIds": ["other_domain"]}},
                        }
                    },
                ],
            }
        )
        assert list(GroupIO.get_dependencies(group)) == [
            (DomainIO, ExternalId(external_id="my_domain")),
            (DomainIO, ExternalId(external_id="other_domain")),
        ]

        tr = TransformationYAML.model_validate(
            {
                "externalId": "tr_1",
                "name": "Transformation 1",
                "destination": {"type": "assets"},
                "ignoreNullFields": True,
                "dataDomainExternalId": "my_domain",
            }
        )
        assert (DomainIO, ExternalId(external_id="my_domain")) in list(TransformationIO.get_dependencies(tr))

        wf = WorkflowYAML.model_validate(
            {
                "externalId": "wf_1",
                "dataDomainExternalId": "my_domain",
            }
        )
        assert list(WorkflowIO.get_dependencies(wf)) == [
            (DomainIO, ExternalId(external_id="my_domain")),
        ]


class TestDomainsAPIRetrieve:
    def test_retrieve_does_not_send_ignore_unknown_ids(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = DomainsAPI(HTTPClient(toolkit_config))
        domain = {"externalId": "my_domain", "name": "My Domain"}
        respx_mock.post(toolkit_config.create_api_url("/datadomains/byids")).respond(
            status_code=200, json={"items": [domain]}
        )

        api.retrieve([ExternalId(external_id="my_domain")])

        assert len(respx_mock.calls) == 1
        body = json.loads(gzip.decompress(respx_mock.calls[0].request.content))
        assert "ignoreUnknownIds" not in body

    def test_ignore_unknown_ids_skips_missing(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = DomainsAPI(HTTPClient(toolkit_config))
        domain = {"externalId": "exists", "name": "Exists"}
        error_body = {"error": {"code": 400, "message": "Data domains not found"}}

        def side_effect(request: httpx2.Request) -> httpx2.Response:
            body = json.loads(gzip.decompress(request.content))
            items = body.get("items", [])
            if any(i.get("externalId") == "missing_one" for i in items):
                return httpx2.Response(status_code=400, json=error_body)
            return httpx2.Response(status_code=200, json={"items": [domain]})

        respx_mock.post(toolkit_config.create_api_url("/datadomains/byids")).mock(side_effect=side_effect)

        result = api.retrieve(
            [ExternalId(external_id="exists"), ExternalId(external_id="missing_one")],
            ignore_unknown_ids=True,
        )

        assert len(result) == 1
        assert result[0].external_id == "exists"
