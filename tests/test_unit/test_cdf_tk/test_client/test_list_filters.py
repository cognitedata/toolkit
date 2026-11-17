import json
from collections.abc import Callable
from typing import Any

import pytest
import respx
from httpx2 import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    ConfidenceRange,
    EpochTimestampRange,
    ExtractionPipelineFilter,
    LabelFilter,
    RelationshipFilter,
    TransformationFilter,
    TransformationNotificationFilter,
    TransformationTagFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import Contact, RawTable


@pytest.mark.usefixtures("disable_gzip")
@pytest.mark.parametrize(
    "path, list_items, expected_filter",
    [
        pytest.param(
            "/relationships/list",
            lambda client: client.relationships.list(
                filter=RelationshipFilter(
                    source_external_ids=["pump"],
                    target_types=["timeSeries"],
                    confidence=ConfidenceRange(min_=0.5),
                    labels=LabelFilter(contains_any=["critical"]),
                ),
                limit=10,
            ),
            {
                "sourceExternalIds": ["pump"],
                "targetTypes": ["timeSeries"],
                "confidence": {"min": 0.5},
                "labels": {"containsAny": [{"externalId": "critical"}]},
            },
            id="relationships",
        ),
        pytest.param(
            "/extpipes/list",
            lambda client: client.extraction_pipelines.list(
                filter=ExtractionPipelineFilter(
                    name="pipeline",
                    contacts=[Contact(email="jane@example.com")],
                    raw_tables=[RawTable(db_name="db", table_name="table")],
                    created_time=EpochTimestampRange(min_=1),
                ),
                limit=10,
            ),
            {
                "name": "pipeline",
                "contacts": [{"email": "jane@example.com"}],
                "rawTables": [{"dbName": "db", "tableName": "table"}],
                "createdTime": {"min": 1},
            },
            id="extraction pipelines",
        ),
        pytest.param(
            "/transformations/filter",
            lambda client: client.transformations.list(
                filter=TransformationFilter(
                    is_public=False,
                    destination_type="nodes",
                    last_updated_time=EpochTimestampRange(max_=2),
                    tags=TransformationTagFilter(contains_any=["daily"]),
                ),
                limit=10,
            ),
            {
                "isPublic": False,
                "destinationType": "nodes",
                "lastUpdatedTime": {"max": 2},
                "tags": {"containsAny": ["daily"]},
            },
            id="transformations",
        ),
        pytest.param(
            "/functions/schedules/list",
            lambda client: client.functions.schedules.list(
                function_external_id="my_function",
                created_time=EpochTimestampRange(min_=1, max_=2),
                cron_expression="0 * * * *",
                limit=10,
            ),
            {"functionExternalId": "my_function", "createdTime": {"min": 1, "max": 2}, "cronExpression": "0 * * * *"},
            id="function schedules",
        ),
        pytest.param(
            "/datasets/list",
            lambda client: client.datasets.list(
                write_protected=True,
                created_time=EpochTimestampRange(min_=1),
                last_updated_time=EpochTimestampRange(max_=2),
                limit=10,
            ),
            {"writeProtected": True, "createdTime": {"min": 1}, "lastUpdatedTime": {"max": 2}},
            id="data sets",
        ),
    ],
)
def test_list_sends_filter_in_body(
    path: str,
    list_items: Callable[[ToolkitClient], object],
    expected_filter: dict[str, Any],
    respx_mock: respx.MockRouter,
    toolkit_config: ToolkitClientConfig,
) -> None:
    respx_mock.post(toolkit_config.create_api_url(path)).mock(
        return_value=Response(status_code=200, json={"items": []})
    )

    list_items(ToolkitClient(config=toolkit_config))

    assert json.loads(respx_mock.calls[0].request.content) == {"filter": expected_filter, "limit": 10}


@pytest.mark.parametrize(
    "path, list_items, expected_params",
    [
        pytest.param(
            "/hostedextractors/jobs",
            lambda client: client.hosted_extractors.jobs.list(source="my_source", mapping="my_mapping", limit=10),
            {"source": "my_source", "mapping": "my_mapping", "limit": "10"},
            id="hosted extractor jobs",
        ),
        pytest.param(
            "/transformations/schedules",
            lambda client: client.transformations.schedules.list(include_public=False, limit=10),
            {"includePublic": "false", "limit": "10"},
            id="transformation schedules",
        ),
        pytest.param(
            "/transformations/notifications",
            lambda client: client.transformations.notifications.list(
                filter=TransformationNotificationFilter(transformation_id=7), limit=10
            ),
            {"transformationId": "7", "limit": "10"},
            id="transformation notifications",
        ),
    ],
)
def test_list_sends_filter_as_query_parameters(
    path: str,
    list_items: Callable[[ToolkitClient], object],
    expected_params: dict[str, str],
    respx_mock: respx.MockRouter,
    toolkit_config: ToolkitClientConfig,
) -> None:
    respx_mock.get(toolkit_config.create_api_url(path)).mock(return_value=Response(status_code=200, json={"items": []}))

    list_items(ToolkitClient(config=toolkit_config))

    assert dict(respx_mock.calls[0].request.url.params) == expected_params


def test_list_function_schedules_rejects_both_function_id_and_external_id(toolkit_config: ToolkitClientConfig) -> None:
    client = ToolkitClient(config=toolkit_config)

    with pytest.raises(ValueError, match="Only one of function_id and function_external_id"):
        client.functions.schedules.list(function_id=1, function_external_id="my_function")
