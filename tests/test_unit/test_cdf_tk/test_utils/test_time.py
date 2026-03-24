from cognite_toolkit._cdf_tk.utils.time import time_windows_ms


class TestTimeWindowsMs:
    def test_none_interval_returns_single_window(self) -> None:
        assert time_windows_ms(0, 1_000, None) == [(0, 1_000)]

    def test_splits_evenly_by_interval(self) -> None:
        hour_ms = 60 * 60 * 1000
        now_ms = 3 * hour_ms
        windows = time_windows_ms(0, now_ms, hour_ms)
        assert windows == [(0, hour_ms), (hour_ms, 2 * hour_ms), (2 * hour_ms, now_ms)]
        for i in range(len(windows) - 1):
            assert windows[i][1] == windows[i + 1][0]
