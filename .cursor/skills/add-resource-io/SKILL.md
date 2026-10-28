---
name: add-resource-io
description: >-
  Add support for a new CDF resource type to Toolkit: resource classes, a CDFResourceAPI subclass registered on
  ToolkitClient, smoke tests in tests_smoke/test_client/test_cdf_resource_api.py, and a ResourceIO subclass with
  YAML class so the resource can be built/deployed/pulled/cleaned. Use when the user asks to add a new resource,
  resource IO, CRUD, loader, or API endpoint to Toolkit.
---

# Add a Resource IO

A new resource is added in **two PRs** (this is the established pattern, e.g. SAP writeback and integrations):

1. **Client PR** (`[prep CDF-XXXX] ...`, Bump: Skip) – resource classes + API + `ToolkitClient` + unit + smoke tests.
2. **IO PR** (`[CDF-XXXX] ...`, Bump: Patch, with changelog) – YAML class + `ResourceIO` + registration + tests.

Ask the user whether to do both now or only the client part. Before writing code, fetch/read the API spec
(`https://api-docs.cognite.com/20230101/tag/<Tag>` or `-beta` / `-alpha`) to get paths, item limits, request and
response fields, and whether update/upsert, `ignoreUnknownIds`, and cursor pagination are supported.

Read one existing, similar implementation end to end before starting. Good references:

| Shape of the API                        | Reference                                                        |
|-----------------------------------------|------------------------------------------------------------------|
| Simple create/retrieve/delete/list      | `client/api/labels.py`, `resource_ios/_data_organization.py`     |
| create/update (patch/replace) + alpha   | `client/api/integrations.py`, `resource_ios/_integrations.py`    |
| Upsert / beta, special upload           | `client/api/skills.py`, `resource_ios/_skill.py`                 |
| Several sub-APIs under one group        | `client/api/sap_writeback.py`, `resource_ios/_sap_writeback.py`  |

All paths below are relative to `cognite_toolkit/_cdf_tk/` unless they start with `tests`.

---

## Part 1 – Client (API) layer

### 1.1 Identifier (only if needed)

Reuse `ExternalId`, `InternalId`, etc. from `client/identifiers`. Only if the resource has a composite or unusual
identifier, add a subclass of `Identifier` in `client/identifiers/_identifiers.py` (implement `__str__` and
`_as_filename`) and export it in `client/identifiers/__init__.py` (import + `__all__`).

### 1.2 Resource classes – `client/resource_classes/<resource>.py`

Module docstring with link to the API docs. Pattern:

```python
from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject, RequestResource, ResponseResource, UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class MyThing(BaseModelObject):            # shared fields (snake_case, aliases are automatic camelCase)
    external_id: str
    name: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class MyThingRequest(MyThing, UpdatableRequestResource):   # RequestResource if the API has no update
    ...                                                      # set container_fields / non_nullable_fields if needed


class MyThingResponse(MyThing, ResponseResource[MyThingRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[MyThingRequest]:
        return MyThingRequest
```

- Response-only fields (timestamps, server ids, status) go only on the response class.
- Write-only fields (e.g. secrets) go on the request; note that such resources cannot round-trip response → request.

### 1.3 API class – `client/api/<resource>.py`

Subclass `CDFResourceAPI[<Response>]` from `client/cdf_client/api.py`:

```python
import builtins
from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.my_thing import MyThingRequest, MyThingResponse


class MyThingsAPI(CDFResourceAPI[MyThingResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/mythings", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/mythings/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/mythings/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/mythings/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/mythings", item_limit=1000),
            },
            # api_version="alpha" / "beta" for non-GA APIs (sets the cdf-version header).
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[MyThingResponse]:
        return PagedResponse[MyThingResponse].model_validate_json(response.body)

    def create(self, items: Sequence[MyThingRequest]) -> builtins.list[MyThingResponse]:
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[MyThingResponse]:
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def update(
        self, items: Sequence[MyThingRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[MyThingResponse]:
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[MyThingResponse]:
        return self._paginate(limit=limit, cursor=cursor)

    def iterate(self, limit: int | None = None) -> Iterable[builtins.list[MyThingResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> builtins.list[MyThingResponse]:
        return self._list(limit=limit)
```

Rules:

- Keys in `method_endpoint_map` must be `APIMethod` literals (`create`, `retrieve`, `update`, `delete`, `list`,
  `upsert`, `search`, `aggregate`, `inspect`). `item_limit` is the API's max items per request / page size.
- Use `builtins.list` in annotations because the class defines `list`.
- If the API lacks `ignoreUnknownIds`, use `_request_item_split_retries` to emulate it.
- Pagination: `_paginate` puts `limit`/`cursor` in query params for `GET` and in the body for `POST`.
- Method names `create`, `retrieve`, `update`, `delete`, `list` matter: the generic smoke test discovers them via
  `getattr`. `retrieve`/`delete` must accept a list of the identifiers returned by `request.as_id()`.
- Sub-APIs (e.g. `client.workflows.versions`) are attributes created in the parent API's `__init__`.

### 1.4 Register on `ToolkitClient` – `client/_toolkit_client.py`

Add `from .api.<resource> import MyThingsAPI` (alphabetical) and `self.my_things = MyThingsAPI(http_client)` in
`ToolkitClient.__init__`.

### 1.5 Mock client – `client/testing.py`

Import the API and add `self.my_things = MagicMock(spec_set=MyThingsAPI)` to `ToolkitClientMock.__init__`
(also mock sub-APIs, e.g. `self.my_things.versions = MagicMock(spec_set=...)`).

### 1.6 Unit tests

- `tests/test_unit/test_cdf_tk/test_client/data.py`:
  - add a minimum response example to `get_example_minimum_responses` (camelCase keys, include required response
    fields like `createdTime`).
  - yield a `CDFResource(response_cls=..., request_cls=..., example_data=..., api_class=MyThingsAPI)` param in
    `iterate_cdf_resources()`. Set `is_as_request_possible=False` / `is_dump_equal_to_example=False` only if needed.
- For non-trivial API logic (custom endpoints, query params, update payload), add
  `tests/test_unit/test_cdf_tk/test_client/test_<resource>.py` using `respx_mock` and
  `@pytest.mark.usefixtures("disable_gzip")`. See `test_integrations.py`.

### 1.7 Smoke tests – `tests_smoke/test_client/test_cdf_resource_api.py`

`test_all_cdf_resource_apis_registered` fails if a new `CDFResourceAPI` subclass is neither generically tested nor
listed in `NOT_GENERIC_TESTED`. Choose one:

**A. Generic CRUDL (preferred)** – works when the API has no dependencies, `create` returns the created item, the
response can be turned back into a request, and `delete` works. Just add the request example to
`get_examples_minimum_requests` (import the response class at the top):

```python
MyThingResponse: [{"externalId": "smoke-test-my-thing", "name": "Smoke Test My Thing"}],
```

`test_crudl` then automatically calls `create`, `retrieve`, `update`, `list`, `delete` (whichever exist).
Use `"dataSetId": ...` in the example if needed; it is replaced with the smoke dataset id.

**B. Dedicated test** – when there are dependencies (parent resource, file, timeseries), no delete, special list
arguments, alpha/beta flags, etc. Add the API class to `NOT_GENERIC_TESTED` with a comment explaining why, still
add the request example to `get_examples_minimum_requests`, and add a method to `TestCDFResourceAPI`:

```python
def test_my_things_crudl(self, toolkit_client: ToolkitClient) -> None:
    client = toolkit_client
    request = MyThingRequest.model_validate(get_examples_minimum_requests(MyThingResponse)[0])
    item_id = request.as_id()
    endpoints = client.my_things._method_endpoint_map

    client.my_things.delete([item_id], ignore_unknown_ids=True)  # clean up leftovers from earlier runs
    try:
        self.assert_endpoint_method(lambda: client.my_things.create([request]), "create", endpoints["create"], item_id)
        self.assert_endpoint_method(lambda: client.my_things.retrieve([item_id]), "retrieve", endpoints["retrieve"], item_id)
        self.assert_endpoint_method(lambda: client.my_things.update([request]), "update", endpoints["update"], item_id)
        try:
            listed = self.wait_until_has_value(lambda: client.my_things.list(limit=1))
        except ToolkitAPIError as e:
            raise EndpointAssertionError(endpoints["list"].path, f"List method failed with error: {e!s}") from e
        if not listed:
            raise EndpointAssertionError(endpoints["list"].path, "Expected at least 1 listed item, got 0")
    finally:
        client.my_things.delete([item_id], ignore_unknown_ids=True)
```

- Always wrap failures in `EndpointAssertionError(endpoint.path, message)` so the report shows which endpoint broke.
- For APIs that might not be enabled on the project, probe with `list(limit=1)` and `pytest.skip` on 403
  (see `test_rulesets_crudl`).
- Use `smoke-test-` prefixed identifiers. Dependencies should come from existing fixtures (`smoke_dataset`,
  `smoke_space`, `smoke_view`, `smoke_timeseries`, ...) or be created and cleaned up inside the test.

Smoke tests run against a real CDF project; run them only if the user has credentials configured:
`uv run pytest tests_smoke/test_client/test_cdf_resource_api.py -k "MyThings or my_things or registered"`.

---

## Part 2 – Resource IO layer

### 2.1 YAML class – `yaml_classes/<resource>.py`

Subclass `ToolkitResource` (from `.base`). This is the user-facing file format: use `Field(description=...)` on every
field, replace internal ids with external ids (e.g. `data_set_external_id` instead of `data_set_id`), and implement
`as_id()`. Export it in `yaml_classes/__init__.py` (import + `__all__`).

### 2.2 IO class – `resource_ios/_<resource>.py`

Subclass `ResourceIO[T_Identifier, T_Request, T_Response, T_YAML]` from `resource_ios/_base_ios.py` and mark it
`@final`:

```python
@final
class MyThingIO(ResourceIO[ExternalId, MyThingRequest, MyThingResponse, MyThingYAML]):
    folder_name = "my_things"           # existing folder if it fits, otherwise a new one (see 2.3)
    kind = "MyThing"                    # files are named <name>.MyThing.yaml
    resource_cls = MyThingResponse
    resource_write_cls = MyThingRequest
    yaml_cls = MyThingYAML
    dependencies = frozenset({DataSetsIO})   # IOs that must be deployed first
    _doc_url = "MyThings/operation/createMyThings"   # override _doc_base_url for alpha/beta docs

    @property
    def display_name(self) -> str:
        return "my things"
```

Required methods:

| Method | Notes |
| ------ | ----- |
| `get_id(item)` (classmethod) | Accept request, response, identifier, or raw `dict` (camelCase keys). |
| `dump_id(id)` (classmethod) | Usually `id.dump()`. |
| `get_minimum_scope(items)` / `create_acl(actions, scope)` | Yield the ACL(s) from `client/resource_classes/group` needed to read/write. Use data-set scope when the resource has a data set. |
| `get_dependencies(resource: YAML)` (classmethod) | Yield `(OtherIO, identifier)` for each referenced resource (data set, space, parent, ...). |
| `create`, `retrieve`, `delete` | Delegate to `self.client.my_things`. `retrieve`/`delete` use `ignore_unknown_ids=True`; `delete` returns the ids. |
| `update` | Optional. If not supported set `support_update = False`. |
| `_iterate(data_set_external_id, space, parent_ids)` | Yield all responses, used by pull/dump/clean. |

Optional overrides: `as_filename`, `load_resource` (e.g. `dataSetExternalId` → `dataSetId` via
`self.client.lookup`), `dump_resource` (reverse of `load_resource`, and normalize server defaults against `local`
so pull/diff is clean), `split_resource`/`get_extra_files` (sidecar files), `sensitive_strings` (secrets),
`support_drop`, `parent_resource`.

### 2.3 Register – `resource_ios/__init__.py`

- Import the IO and add it to `__all__`. Discovery is automatic via `ResourceIO.__subclasses__()`.
- If the folder is new, add it to the `ResourceTypes` literal.
- If the resource is alpha: add a flag to `Flags` in `feature_flags.py`, add
  `if not FeatureFlag.is_enabled(Flags.X): _EXCLUDED_CRUDS.add(MyThingIO)`, enable it in the repo `cdf.toml`
  `[alpha_flags]`, and discard the folder in `test_resource_types_is_up_to_date` in
  `tests/test_unit/test_cdf_tk/test_cruds/test_base.py` when the flag is off.

### 2.4 Tests and test data

- Example YAML: `tests/data/complete_org/modules/.../<folder>/my.MyThing.yaml`
  (or `tests/data/complete_org_alpha_flags/...` for alpha resources).
- Approval client: add an `APIResource(api_name="my_things", resource_cls=..., _write_cls=..., methods={...})`
  entry to `API_RESOURCES` in `tests/test_unit/approval_client/config.py`.
- Add a small `tests/test_unit/test_cdf_tk/test_cruds/test_<resource>.py` for custom logic (dependencies,
  load/dump conversions). Keep tests minimal.
- Regenerate snapshots when they change: `uv run pytest tests/test_unit/test_cli --force-regen` and review the diff
  (e.g. `test_build_deploy_snapshots/complete_org*.yaml`).

---

## Verify

```bash
uv sync --all-extras
uv run pytest tests/test_unit/test_cdf_tk/test_client tests/test_unit/test_cdf_tk/test_cruds -q
uv run pytest tests/test_unit/test_cli -q
uv run pre-commit run --all-files
```

Follow `AGENTS.md`: no `from __future__ import annotations`, imports at top, Pydantic for data classes, never
`--no-verify`.
