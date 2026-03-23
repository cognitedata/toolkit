"""Rule Set Versions API for managing versions of CDF rule sets."""

from collections import defaultdict
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
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
    Single-version reads use GET /rulesets/{externalId}/versions/{version}.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/rulesets/{externalId}/versions", item_limit=1),
                "delete": Endpoint(method="POST", path="/rulesets/{externalId}/versions/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/rulesets/{externalId}/versions", item_limit=50),
                "retrieve": Endpoint(method="GET", path="/rulesets/{externalId}/versions/{version}", item_limit=1),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RuleSetVersionResponse]:
        return PagedResponse[RuleSetVersionResponse].model_validate_json(response.body)

    @staticmethod
    def _group_by_parent(
        items: Sequence[RuleSetVersionRequest],
    ) -> dict[str, list[RuleSetVersionRequest]]:
        by_parent: dict[str, list[RuleSetVersionRequest]] = {}
        for item in items:
            by_parent.setdefault(item.rule_set_external_id, []).append(item)
        return by_parent

    def create(self, items: Sequence[RuleSetVersionRequest]) -> list[RuleSetVersionResponse]:
        results: list[RuleSetVersionResponse] = []
        for rs_ext_id, group in self._group_by_parent(items).items():
            url = self._make_url(self._method_endpoint_map["create"].path.format(externalId=rs_ext_id))
            for item in group:
                request = RequestMessage(
                    endpoint_url=url,
                    method="POST",
                    body_content={"items": [item.dump()]},
                    api_version=self._api_version,
                )
                response = self._http_client.request_single_retries(request)
                page = self._validate_page_response(response.get_success_or_raise(request))
                for version in page.items:
                    version.rule_set_external_id = rs_ext_id
                results.extend(page.items)
        return results

    def retrieve(
        self,
        items: Sequence[RuleSetVersionId],
        ignore_unknown_ids: bool = False,
    ) -> list[RuleSetVersionResponse]:
        results: list[RuleSetVersionResponse] = []
        for item in items:
            url = self._make_url(
                self._method_endpoint_map["retrieve"].path.format(
                    externalId=item.rule_set_external_id, version=item.version
                )
            )
            request = RequestMessage(
                endpoint_url=url,
                method="GET",
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                ver = RuleSetVersionResponse.model_validate_json(response.body)
                ver.rule_set_external_id = item.rule_set_external_id
                results.append(ver)
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise(request)
        return results

    def delete(self, ids: Sequence[RuleSetVersionId]) -> None:
        by_parent: defaultdict[str, list[str]] = defaultdict(list)
        for id_ in ids:
            by_parent[id_.rule_set_external_id].append(id_.version)
        for rs_ext_id, versions in by_parent.items():
            url = self._make_url(self._method_endpoint_map["delete"].path.format(externalId=rs_ext_id))
            request = RequestMessage(
                endpoint_url=url,
                method="POST",
                body_content={"items": [{"version": v} for v in versions]},
                api_version=self._api_version,
            )
            self._http_client.request_single_retries(request).get_success_or_raise(request)

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
