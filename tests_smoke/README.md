# Smoke Tests

This directory contains smoke tests for the Cognite Toolkit. These tests are designed to run periodically
(e.g., via a scheduled CI job) to verify that:

1. The CDF API documentation URLs are accessible
2. Critical integrations are working correctly

## Structure

```text
tests_smoke/
├── conftest.py              # Pytest fixtures (ToolkitClient setup)
├── check_smoke_results.py   # Script to parse test results and notify Slack
├── tests_cruds/
│   └── tests_resource_curd.py  # CRUD resource validation tests
└── README.md
```

## Tests

### `tests_cruds/tests_resource_curd.py`

- **`test_doc_url_is_valid`**: Validates that all CRUD resource documentation URLs are accessible.
  Uses async HTTP requests in parallel for fast execution. If any URLs fail, they are reported together
  in a single assertion error.

- **`test_workflow_working`**: A canary test that intentionally fails to verify that Slack notifications
  are working correctly.

## Running the Tests

### Prerequisites

1. Create a `.env` file in the repository root with the following variables:

   ```env
   CDF_CLUSTER=<your-cluster>
   CDF_PROJECT=<your-project>
   IDP_TOKEN_URL=<token-url>
   IDP_CLIENT_ID=<client-id>
   IDP_CLIENT_SECRET=<client-secret>
   ```

2. Install dependencies:

   ```bash
   uv sync --group dev
   ```

### Running Tests

```bash
# Run all smoke tests
pytest tests_smoke/

# Run with JSON report (for Slack notification script)
pytest tests_smoke/ --json-report --json-report-file=smoke_results.json
```

## Slack Notifications

The `check_smoke_results.py` script parses pytest JSON reports and sends notifications to Slack
when tests fail.

### Usage

```bash
python tests_smoke/check_smoke_results.py <results_file> [--dry-run]
```

### Required Environment Variables

- `SLACK_WEBHOOK_URL`: Slack webhook URL for sending notifications
- `GITHUB_REPO_URL`: URL to the GitHub repository (included in error messages)

### Notification Behavior

- **On failure**: Sends a message to Slack with the failure details
- **On Monday mornings (UTC)**: Sends an "alive" message confirming tests are running
- **On success (other days)**: No notification sent

## CI Integration

These tests are intended to run on a schedule (e.g., daily) in CI. The typical workflow is:

1. Run pytest with JSON report output
2. Run `check_smoke_results.py` to parse results and notify Slack
3. The script handles both test failures and execution failures (missing report file, invalid JSON, etc.)

## Creating Tests

To add new smoke tests, create new test files in the `tests_smoke/` directory and create a regular `pytest`.
The difference is that you should NOT use `assert` statements directly. Instead, raise `AssertionError`
with a descriptive message, something that is readable, also for a non-technical audience, since the
messages will be sent to Slack.

The reason for this is that `assert` statements will always include the line number and code snippet
in the error message, which can be confusing when the message is sent to Slack. By raising
`AssertionError` directly, you have full control over the error message content.
