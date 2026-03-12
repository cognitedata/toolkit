"""Rule Set Versions API for managing versions of CDF rule sets."""

from collections import defaultdict
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    RequestMessage,
)
from cognite_toolkit._cdf_tk.client.identifiers import RuleSetVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset_version import (
    RuleSetVersionRequest,
    RuleSetVersionResponse,
)


class RuleSetVersionsAPI(CDFResourceAPI[RuleSetVersionResponse]):
    """API for managing rule set versions.

    Endpoints are scoped under a parent rule set: /rulesets/{externalId}/versions.
    Versions are identified by a semantic version string (major.minor.patch).
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/rulesets/{externalId}/versions", item_limit=1),
                "delete": Endpoint(method="POST", path="/rulesets/{externalId}/versions/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/rulesets/{externalId}/versions", item_limit=50),
                "retrieve": Endpoint(method="POST", path="/rulesetversions/byids", item_limit=100),
            },
            api_version="alpha",
        )
        self.versions = RuleSetVersionsAPI(http_client)

    def create(self, items: Sequence[RuleSetVersionRequest]) -> list[RuleSetVersionResponse]:
        results: list[RuleSetVersionResponse] = []
        for rs_ext_id, group in self._group_by_parent(items).items():
            url = self._make_url(self._method_endpoint_map["create"].path.format(externalId=rs_ext_id))
            for item in group:
                response = self._http_client.request_single_retries(
                    RequestMessage(
                        endpoint_url=url,
                        method="POST",
                        body_content={"items": [item.dump()]},
                        api_version=self._api_version,
                    )
                )
                page = self._validate_page_response(response.get_success_or_raise())
                for version in page.items:
                    version.rule_set_external_id = rs_ext_id
                results.extend(page.items)
        return results

    def retrieve(
        self,
        items: Sequence[RuleSetVersionId],
        ignore_unknown_ids: bool = False,
    ) -> list[RuleSetVersionResponse]:
        if not items:
            return []
        body = {"items": [{"externalId": item.rule_set_external_id, "version": item.version} for item in items]}
        url = self._make_url(self._method_endpoint_map["retrieve"].path)
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=url,
                method="POST",
                body_content=body,
                api_version=self._api_version,
            )
        )
        if ignore_unknown_ids:
            success = response.get_success_or_raise()
            page = self._validate_page_response(success)
        else:
            page = self._validate_page_response(response.get_success_or_raise())

        lookup = {item.rule_set_external_id: item.rule_set_external_id for item in items}
        for ver in page.items:
            if not ver.rule_set_external_id:
                ver.rule_set_external_id = lookup.get(ver.version, "")
        return page.items

    def delete(self, ids: Sequence[RuleSetVersionId]) -> None:
        by_parent: defaultdict[str, list[str]] = defaultdict(list)
        for id_ in ids:
            by_parent[id_.rule_set_external_id].append(id_.version)
        for rs_ext_id, versions in by_parent.items():
            url = self._make_url(self._method_endpoint_map["delete"].path.format(externalId=rs_ext_id))
            self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=url,
                    method="POST",
                    body_content={"items": [{"version": v} for v in versions]},
                    api_version=self._api_version,
                )
            ).get_success_or_raise()

    def iterate(self, rule_set_external_id: str, limit: int | None = 50) -> Iterable[list[RuleSetVersionResponse]]:
        path = self._method_endpoint_map["list"].path.format(externalId=rule_set_external_id)
        for batch in self._iterate(limit=limit, endpoint_path=path):
            for item in batch:
                item.rule_set_external_id = rule_set_external_id
            yield batch

    def list(self, rule_set_external_id: str, limit: int | None = 50) -> list[RuleSetVersionResponse]:
        path = self._method_endpoint_map["list"].path.format(externalId=rule_set_external_id)
        items = self._list(limit=limit, endpoint_path=path)
        for item in items:
            item.rule_set_external_id = rule_set_external_id
        return items
