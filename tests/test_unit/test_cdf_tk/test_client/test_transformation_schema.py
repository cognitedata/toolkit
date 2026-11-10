from collections.abc import Callable

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.transformation_schema import TransformationSchemaAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import Column

_COLUMNS = [
    {"name": "externalId", "sqlType": "STRING", "type": "string", "nullable": False},
    {
        "name": "metadata",
        "sqlType": "MAP<STRING, STRING>",
        "type": {"type": "map", "keyType": "string", "valueType": "string", "valueContainsNull": True},
        "nullable": True,
    },
]


class TestTransformationSchemaAPI:
    @pytest.mark.parametrize(
        "call, path, expected_params",
        [
            pytest.param(
                lambda api: api.retrieve("assets", conflict_mode="upsert"),
                "/transformations/schema/assets",
                {"conflictMode": "upsert"},
                id="resource type",
            ),
            pytest.param(
                lambda api: api.retrieve_sequence_rows("my_sequence"),
                "/transformations/schema/sequence_rows",
                {"externalId": "my_sequence"},
                id="sequence rows",
            ),
            pytest.param(
                lambda api: api.retrieve_view_instances(
                    "upsert", view=ViewId(space="sp", external_id="MyView", version="v1"), instance_type="nodes"
                ),
                "/transformations/schema/instances",
                {
                    "conflictMode": "upsert",
                    "viewSpace": "sp",
                    "viewExternalId": "MyView",
                    "viewVersion": "v1",
                    "instanceType": "nodes",
                },
                id="view instances",
            ),
            pytest.param(
                lambda api: api.retrieve_data_model_instances(
                    "upsert", data_model=DataModelId(space="sp", external_id="MyModel", version="v1"), type="MyView"
                ),
                "/transformations/schema/instances",
                {
                    "conflictMode": "upsert",
                    "dataModelSpace": "sp",
                    "dataModelExternalId": "MyModel",
                    "dataModelVersion": "v1",
                    "type": "MyView",
                },
                id="data model instances",
            ),
        ],
    )
    def test_retrieve_schema(
        self,
        call: Callable[[TransformationSchemaAPI], list[Column]],
        path: str,
        expected_params: dict[str, str],
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        api = TransformationSchemaAPI(HTTPClient(toolkit_config))
        respx_mock.get(toolkit_config.create_api_url(path)).mock(
            return_value=httpx2.Response(status_code=200, json={"items": _COLUMNS})
        )

        columns = call(api)

        assert {
            "columns": [column.dump() for column in columns],
            "params": dict(respx_mock.calls[-1].request.url.params),
        } == {"columns": _COLUMNS, "params": expected_params}
