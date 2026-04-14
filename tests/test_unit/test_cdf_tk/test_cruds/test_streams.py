import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.resources_ios._resource_ios.streams import StreamIO

_HOUR_MS = 60 * 60 * 1000
_NOW_MS = 3 * _HOUR_MS

_IMMUTABLE_STREAM = {
    "externalId": "my-stream",
    "createdTime": 0,
    "createdFromTemplate": "ImmutableTestStream",
    "type": "Immutable",
    "settings": {
        "lifecycle": {"retainedAfterSoftDelete": "P30D"},
        "limits": {
            "maxRecordsTotal": {"provisioned": 1_000_000},
            "maxGigaBytesTotal": {"provisioned": 10},
            "maxFilteringInterval": "PT1H",
        },
    },
}

_MUTABLE_STREAM = {
    "externalId": "my-stream",
    "createdTime": 0,
    "createdFromTemplate": "BasicLiveData",
    "type": "Mutable",
}


class TestStreamCRUDIterLastUpdatedTimeWindows:
    @pytest.mark.parametrize(
        "start_ms, expected",
        [
            pytest.param(
                None,
                [
                    {"gte": 0, "lt": _HOUR_MS},
                    {"gte": _HOUR_MS, "lt": 2 * _HOUR_MS},
                    {"gte": 2 * _HOUR_MS, "lt": _NOW_MS},
                ],
                id="uses-created-time-as-lower-bound",
            ),
            pytest.param(
                _HOUR_MS,
                [{"gte": _HOUR_MS, "lt": 2 * _HOUR_MS}, {"gte": 2 * _HOUR_MS, "lt": _NOW_MS}],
                id="explicit-start-ms-overrides-created-time",
            ),
        ],
    )
    def test_immutable_stream(
        self,
        monkeypatch: pytest.MonkeyPatch,
        start_ms: int | None,
        expected: list[dict[str, int]],
    ) -> None:
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.resources_ios._resource_ios.streams.time.time", lambda: _NOW_MS / 1000
        )
        with monkeypatch_toolkit_client() as client:
            client.streams.retrieve.return_value = [StreamResponse.model_validate(_IMMUTABLE_STREAM)]
            windows = StreamIO.create_loader(client).last_updated_time_windows("my-stream", start_ms=start_ms)

        assert windows == expected

    @pytest.mark.parametrize(
        "start_ms, expected",
        [
            pytest.param(None, [None], id="no-filter-needed"),
            pytest.param(_HOUR_MS, [{"gte": _HOUR_MS, "lt": _NOW_MS}], id="bounded-by-start-ms"),
        ],
    )
    def test_mutable_stream(
        self,
        monkeypatch: pytest.MonkeyPatch,
        start_ms: int | None,
        expected: list[dict[str, int] | None],
    ) -> None:
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.resources_ios._resource_ios.streams.time.time", lambda: _NOW_MS / 1000
        )
        with monkeypatch_toolkit_client() as client:
            client.streams.retrieve.return_value = [StreamResponse.model_validate(_MUTABLE_STREAM)]
            windows = StreamIO.create_loader(client).last_updated_time_windows("my-stream", start_ms=start_ms)

        assert windows == expected

    def test_unknown_stream_yields_no_windows(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.streams.retrieve.return_value = []
            windows = StreamIO.create_loader(client).last_updated_time_windows("unknown-stream")

        assert windows == []
