import pytest

from cognite_toolkit._cdf_tk.utils.cron import MINUTES_PER_DAY, MINUTES_PER_WEEK, cron_minutes_of_week

MONDAY, SATURDAY, SUNDAY = 0, 5, 6


class TestCronMinutesOfWeek:
    @pytest.mark.parametrize(
        "expression, utc_offset_minutes, expected",
        [
            pytest.param(
                "30 2 * * *", 0, {day * MINUTES_PER_DAY + 150 for day in range(7)}, id="Daily at 02:30 every day"
            ),
            pytest.param("*/20 0 * * MON", 0, {0, 20, 40}, id="Step and weekday name"),
            pytest.param("0 0 * * 0,6", 0, {SATURDAY * MINUTES_PER_DAY, SUNDAY * MINUTES_PER_DAY}, id="Sunday as 0"),
            pytest.param("0 0 * * 1-2", 60, {MINUTES_PER_WEEK - 60, MONDAY * MINUTES_PER_DAY + 23 * 60}, id="Offset"),
            pytest.param("0 0 1 * *", 0, set(), id="Day of month does not run every week"),
            pytest.param("0 0 * 1 *", 0, set(), id="Month does not run every week"),
            pytest.param("0 0 * 1-6 MON", 0, set(), id="Weekday in some months does not run every week"),
            pytest.param("0 0 1 * MON", 0, {MONDAY * MINUTES_PER_DAY}, id="Day of month or weekday runs on weekday"),
            pytest.param("0 0 1-31 JAN-DEC *", 0, {day * MINUTES_PER_DAY for day in range(7)}, id="Full ranges"),
            pytest.param("@monthly", 0, set()),
            pytest.param("@yearly", 0, set()),
            pytest.param("@hourly", 0, {day * MINUTES_PER_DAY + hour * 60 for day in range(7) for hour in range(24)}),
        ],
    )
    def test_cron_minutes_of_week(self, expression: str, utc_offset_minutes: int, expected: set[int]) -> None:
        assert cron_minutes_of_week(expression, utc_offset_minutes) == expected

    @pytest.mark.parametrize("expression", ["0 0 * *", "60 * * * *", "0 0 L * *", "*/0 * * * *"])
    def test_invalid_expression(self, expression: str) -> None:
        with pytest.raises(ValueError):
            cron_minutes_of_week(expression)
