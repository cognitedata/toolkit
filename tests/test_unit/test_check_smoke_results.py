import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import respx
from httpx import Response

from tests_smoke.check_smoke_results import Context, SlackMessage, check_smoke_tests_results


@pytest.fixture
def context() -> Context:
    """Fixture to provide a dummy Context object."""
    return Context(
        slack_webhook_url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
        github_repo_url="https://github.com/cognitedata/neat",
        # Not Monday morning
        now=datetime(2025, 11, 25, 12, 0, 0),
    )


@pytest.fixture
def call_slack(respx_mock: respx.MockRouter, context: Context) -> Iterator[respx.MockRouter]:
    """Fixture to mock Slack webhook calls."""
    respx_mock.post(url=context.slack_webhook_url).mock(Response(status_code=200))
    yield respx_mock


@pytest.fixture
def all_tests_passing() -> dict[str, Any]:
    """Fixture to provide a valid pytest report format."""
    return {
        "created": 1700000000.0,
        "duration": 12.34,
        "exitcode": 0,
        "root": "/path/to/tests",
        "environment": {},
        "summary": {
            "collected": 3,
            "total": 3,
            "passed": 2,
            "failed": 1,
            "skipped": 0,
            "errors": 0,
        },
        "tests": [
            {"nodeid": "test_one", "outcome": "passed", "lineno": 10},
            {"nodeid": "test_two", "outcome": "passed", "lineno": 20},
            {"nodeid": "test_three", "outcome": "passed", "lineno": 30},
        ],
    }


def failed_tests_report_cases() -> Iterator[tuple]:
    yield pytest.param(
        {
            "created": 1700000000.0,
            "duration": 12.34,
            "exitcode": 1,
            "root": "/path/to/tests",
            "environment": {},
            "summary": {
                "collected": 3,
                "total": 3,
                "passed": 1,
                "failed": 1,
                "skipped": 0,
                "errors": 0,
            },
            "tests": [
                {"nodeid": "test_one", "outcome": "passed", "lineno": 10},
                {
                    "nodeid": "test_two",
                    "outcome": "failed",
                    "lineno": 20,
                    "call": {
                        "duration": 0.5,
                        "outcome": "failed",
                        "crash": {
                            "path": "/path/to/tests/test_file.py",
                            "lineno": 42,
                            "message": "AssertionError: expected 1 but got 0",
                        },
                    },
                },
            ],
        },
        [SlackMessage(topic="CDF API", message="expected 1 but got 0")],
        id="single_failed_test",
    )


class TestCheckSmokeResults:
    def test_file_does_not_exist(self, call_slack: respx.MockRouter, context: Context) -> None:
        """Test that the function handles a non-existent file gracefully."""
        check_smoke_tests_results(Path("non_existent_file.txt"), context)

        assert len(call_slack.calls) == 1
        request = call_slack.calls[0].request
        assert "Smoke tests failed to execute" in request.content.decode()

    def test_invalid_file_format(self, call_slack: respx.MockRouter, context: Context) -> None:
        """Test that the function handles an invalid file format."""
        report_file = MagicMock(spec=Path)
        report_file.is_file.return_value = True
        report_file.read_text.return_value = "invalid json content"

        check_smoke_tests_results(report_file, context)

        assert len(call_slack.calls) == 1
        request = call_slack.calls[0].request
        assert "is not a valid pytest report" in request.content.decode()

    def test_send_alive_message(
        self, all_tests_passing: dict[str, Any], call_slack: respx.MockRouter, context: Context
    ) -> None:
        monday_morning = datetime(2024, 6, 3, 8, 0, 0)  # A Monday at 8 AM UTC
        report_file = MagicMock(spec=Path)
        report_file.is_file.return_value = True
        report_file.name = "report.json"
        report_file.read_text.return_value = json.dumps(all_tests_passing)
        context.now = monday_morning
        check_smoke_tests_results(report_file, context)

        assert len(call_slack.calls) == 1
        request = call_slack.calls[0].request
        assert "Smoke tests are running fine" in request.content.decode()

    @pytest.mark.parametrize("report_data, expected_messages", failed_tests_report_cases())
    def test_valid_format_expected_requests(
        self,
        report_data: dict[str, Any],
        expected_messages: list[SlackMessage],
        call_slack: respx.MockRouter,
        context: Context,
    ) -> None:
        """Test that the function sends expected requests for a valid report file."""
        report_file = MagicMock(spec=Path)
        report_file.is_file.return_value = True
        report_file.name = "report.json"
        report_file.read_text.return_value = json.dumps(report_data)

        check_smoke_tests_results(report_file, context)

        assert len(call_slack.calls) == len(expected_messages)
        actual_messages = [
            SlackMessage.model_validate_json(call_slack.calls[i].request.content.decode())
            for i in range(len(call_slack.calls))
        ]
        assert [msg.model_dump() for msg in actual_messages] == [msg.model_dump() for msg in expected_messages]
