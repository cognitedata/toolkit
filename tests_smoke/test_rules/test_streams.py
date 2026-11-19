from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.rules._streams import StreamLimits


def test_stream_limits(toolkit_client: ToolkitClient) -> None:
    limit_responses = toolkit_client.limits.list(prefix="streams.")
    try:
        limits = StreamLimits.from_responses(limit_responses)
    except ValidationError as e:
        raise AssertionError(f"StreamLimits validation failed. Check the retrieved stream limit values. {e}")
    assert limits.max_active_streams_by_template, "Expected per-template stream limits"
