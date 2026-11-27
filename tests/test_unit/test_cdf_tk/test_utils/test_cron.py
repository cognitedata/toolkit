from datetime import UTC, datetime

import pytest

from cognite_toolkit._cdf_tk.utils.cron import cron_starts


def _utc(month: int, day: int, hour: int = 0, minute: int = 0, year: int = 2026) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


class TestCronStarts:
    @pytest.mark.parametrize(
        "expression, start, end, timezone, expected",
        [
            pytest.param("30 2 * * *", _utc(12, 1), _utc(12, 3), None, [_utc(12, 1, 2, 30), _utc(12, 2, 2, 30)]),
            pytest.param("*/20 0 * * TUE", _utc(12, 1), _utc(12, 2), None, [_utc(12, 1, 0, m) for m in (0, 20, 40)]),
            pytest.param("0 0 * * 7", _utc(11, 30), _utc(12, 7), None, [_utc(12, 6)], id="Sunday as 7"),
            pytest.param("@monthly", _utc(11, 15), _utc(12, 15), None, [_utc(12, 1)]),
            pytest.param("0 0 * 1 *", _utc(12, 1), _utc(12, 31), None, [], id="Outside the months"),
            pytest.param(
                "0 0 1 * MON",
                _utc(11, 29),
                _utc(12, 8),
                None,
                [_utc(11, 30), _utc(12, 1), _utc(12, 7)],
                id="Day of month or weekday",
            ),
            pytest.param("0 6 * * *", _utc(12, 1), _utc(12, 2), "Europe/Oslo", [_utc(12, 1, 5)], id="Winter time"),
            pytest.param("0 6 * * *", _utc(7, 1), _utc(7, 2), "Europe/Oslo", [_utc(7, 1, 4)], id="Summer time"),
            pytest.param(
                "30 2 * * *",
                _utc(3, 28, year=2027),
                _utc(3, 29, year=2027),
                "Europe/Oslo",
                [_utc(3, 28, 1, 30, year=2027)],
                id="Skipped by daylight saving time",
            ),
            pytest.param("0 0 * * *", _utc(12, 1), _utc(12, 2), "Not/AZone", [_utc(12, 1)], id="Unknown zone as UTC"),
        ],
    )
    def test_cron_starts(
        self, expression: str, start: datetime, end: datetime, timezone: str | None, expected: list[datetime]
    ) -> None:
        assert cron_starts(expression, start, end, timezone) == expected

    @pytest.mark.parametrize("expression", ["0 0 * *", "60 * * * *", "0 0 L * *", "*/0 * * * *"])
    def test_invalid_expression(self, expression: str) -> None:
        with pytest.raises(ValueError):
            cron_starts(expression, _utc(12, 1), _utc(12, 2))
