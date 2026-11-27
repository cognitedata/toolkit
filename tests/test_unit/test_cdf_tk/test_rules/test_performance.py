from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
from cognite_toolkit._cdf_tk.data_classes._insights import BuildWarning
from cognite_toolkit._cdf_tk.rules import PerformanceRuleSet, ValidationInput, _performance
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms, ms_to_datetime

NOW_DATETIME = datetime(2026, 12, 1, 0, 5, tzinfo=UTC)
NOW = datetime_to_ms(NOW_DATETIME)
DAY_MS = int(timedelta(days=1).total_seconds() * 1000)
MINUTE_MS = 60 * 1000


def _schedule(external_id: str, interval: str, is_paused: bool = False) -> TransformationScheduleResponse:
    return TransformationScheduleResponse(
        id=1, external_id=external_id, interval=interval, is_paused=is_paused, created_time=0, last_updated_time=0
    )


def _job(external_id: str, started_time: int, minutes: int) -> TransformationJobResponse:
    return TransformationJobResponse.model_validate(
        {
            "id": 1,
            "uuid": "1",
            "status": "Completed",
            "transformationId": 1,
            "transformationExternalId": external_id,
            "sourceProject": "my_project",
            "destinationProject": "my_project",
            "destination": {"type": "assets"},
            "conflictMode": "upsert",
            "query": "SELECT 1",
            "ignoreNullFields": True,
            "createdTime": started_time,
            "startedTime": started_time,
            "finishedTime": started_time + minutes * MINUTE_MS,
        }
    )


def _execution(id_: int, start_time: int, end_time: int) -> WorkflowExecutionResponse:
    return WorkflowExecutionResponse(
        id=str(id_),
        workflow_external_id=f"wf_{id_}",
        version="v1",
        status="COMPLETED",
        created_time=start_time,
        start_time=start_time,
        end_time=end_time,
    )


@pytest.fixture(autouse=True)
def fixed_now(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_performance, "_utc_now", lambda: NOW_DATETIME)


class TestPerformanceRuleSet:
    def test_schedules_overlapping_by_their_run_time(self) -> None:
        client = MagicMock()
        long_running = [f"tr_{no}" for no in range(6)]
        quarter_past = [f"tr_{no}" for no in range(6, 11)]
        client.transformations.schedules.list.return_value = [
            *(_schedule(external_id, "0 * * * *") for external_id in long_running),
            *(_schedule(external_id, "15 * * * *") for external_id in quarter_past),
            _schedule("tr_paused", "15 * * * *", is_paused=True),
        ]
        # One job a day apart, such that they did not run at the same time. Without their 30 minute run time, at
        # most 6 would run at the same time.
        client.transformations.jobs.iterate.return_value = [
            [_job(external_id, NOW - (no + 1) * DAY_MS, minutes=30) for no, external_id in enumerate(long_running)]
        ]

        insights = list(PerformanceRuleSet(client=client).validate(ValidationInput()))

        assert [
            (insight.code, insight.source_url, insight.message) if isinstance(insight, BuildWarning) else insight
            for insight in insights
        ] == [
            (
                "CONGESTED-SCHEDULE",
                "/transformations/",
                "Up to 11 transformations are scheduled to run at the same time, next at 2026-12-01 00:15 UTC: "
                "'tr_0', 'tr_1', 'tr_10', 'tr_2', 'tr_3', 'tr_4', 'tr_5', 'tr_6', 'tr_7', 'tr_8' and 1 more. "
                "More than 10 run at the same time during 744 minutes in the next 31 days, based on their schedules "
                "and median run time in the last 7 days. Running many at once can exceed concurrency limits, which "
                "delays or fails runs.",
            )
        ]

    @pytest.mark.parametrize("run_count, expected_warnings", [(10, 0), (11, 1)])
    def test_runs_at_the_same_time(self, run_count: int, expected_warnings: int) -> None:
        client = MagicMock()
        start = NOW - DAY_MS
        client.workflows.executions.list.return_value = [
            _execution(no, start + no * MINUTE_MS, start + 60 * MINUTE_MS) for no in range(run_count)
        ]
        peak_time = ms_to_datetime(start + (run_count - 1) * MINUTE_MS).strftime("%Y-%m-%d %H:%M")

        insights = list(PerformanceRuleSet(client=client).validate(ValidationInput()))

        assert [insight.message for insight in insights] == [
            f"11 runs of workflows were running at the same time at {peak_time} UTC, the most in the last 7 days: "
            "'wf_0', 'wf_1', 'wf_10', 'wf_2', 'wf_3', 'wf_4', 'wf_5', 'wf_6', 'wf_7', 'wf_8' and 1 more. Running "
            "many at once can exceed concurrency limits, which delays or fails runs."
        ][:expected_warnings]
