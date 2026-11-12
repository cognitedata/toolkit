from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightDefinition
from cognite_toolkit._cdf_tk.rules import TransformationRuleSet

API_URL = "https://bluefield.cognitedata.com/api/v1/projects/my_project"


def _transformation(id_: int, external_id: str, **extra: Any) -> TransformationResponse:
    return TransformationResponse.model_validate(
        {
            "id": id_,
            "externalId": external_id,
            "name": external_id,
            "ignoreNullFields": True,
            "createdTime": 0,
            "lastUpdatedTime": 0,
            "query": "SELECT 1",
            "isPublic": True,
            "conflictMode": "upsert",
            "destination": {"type": "assets"},
            "owner": "me",
            "ownerIsCurrentUser": True,
            "hasSourceOidcCredentials": False,
            "hasDestinationOidcCredentials": False,
            **extra,
        }
    )


def _session_info(session_id: int) -> dict[str, Any]:
    return {"sessionId": session_id, "projectName": "my_project"}


def _client() -> MagicMock:
    client = MagicMock()
    client.config.create_api_url.side_effect = lambda endpoint: f"{API_URL}{endpoint}"
    client.transformations.list.return_value = [
        _transformation(1, "tr_healthy", sourceSession=_session_info(10), destinationSession=_session_info(11)),
        _transformation(2, "tr_blocked", blocked={"reason": "Too many failures", "createdTime": 0}),
        _transformation(3, "tr_in_workflow", sourceSession=_session_info(12)),
    ]
    client.transformations.schedules.list.return_value = [
        TransformationScheduleResponse(
            id=1, external_id="tr_healthy", interval="0 * * * *", is_paused=False, created_time=0, last_updated_time=0
        )
    ]
    client.workflows.versions.list.return_value = [
        WorkflowVersionResponse.model_validate(
            {
                "workflowExternalId": "my_workflow",
                "version": "v1",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "inline",
                            "type": "subworkflow",
                            "parameters": {
                                "subworkflow": {
                                    "tasks": [
                                        {
                                            "externalId": "run_tr",
                                            "type": "transformation",
                                            "parameters": {
                                                "transformation": {
                                                    "externalId": "tr_in_workflow",
                                                    "useTransformationCredentials": True,
                                                }
                                            },
                                        }
                                    ]
                                }
                            },
                        }
                    ]
                },
            }
        )
    ]
    client.sessions.retrieve.return_value = [
        SessionResponse(id=10, status="ACTIVE"),
        SessionResponse(id=11, status="ACTIVE"),
        SessionResponse(id=12, status="EXPIRED"),
    ]
    return client


class TestTransformationRuleSet:
    def test_validate_transformations_in_cdf(self) -> None:
        rule = TransformationRuleSet(client=_client())

        insights = sorted(
            (insight.code, insight.source_url) for insight in rule.validate() if isinstance(insight, InsightDefinition)
        )

        assert insights == [
            ("BLOCKED-RESOURCE", f"{API_URL}/transformations/2"),
            ("INVALID-CREDENTIALS", f"{API_URL}/transformations/3"),
            ("MISSING-CREDENTIALS", f"{API_URL}/transformations/3"),
            ("UNTRIGGERED-RESOURCE", f"{API_URL}/transformations/2"),
        ]

    def test_unverified_credentials_when_sessions_cannot_be_retrieved(self) -> None:
        client = _client()
        client.sessions.retrieve.side_effect = ToolkitAPIError("Forbidden", code=403)
        rule = TransformationRuleSet(client=client)

        codes = {insight.code for insight in rule.validate()}

        assert codes == {"BLOCKED-RESOURCE", "MISSING-CREDENTIALS", "UNTRIGGERED-RESOURCE", "UNVERIFIED-CREDENTIALS"}

    def test_planned_in_cdf_context_without_resources(self) -> None:
        plan = ValidationCommand._create_validation_plan(client=MagicMock(), context="cdf")

        assert TransformationRuleSet in {type(step.rule) for step in plan}
