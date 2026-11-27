import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    EpochTimestampRange,
    FunctionCallFilter,
    WorkflowExecutionFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import ScheduleTriggerRule
from cognite_toolkit._cdf_tk.data_classes._insights import BuildWarning, Insight
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionIO,
    FunctionScheduleIO,
    TransformationIO,
    TransformationScheduleIO,
    WorkflowIO,
    WorkflowTriggerIO,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.cron import cron_starts
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms, ms_to_datetime

from ._base import InternalValidatorException, RuleSetStatus, ToolkitRuleSet, ValidationInput

# Warn when more than this many compute resources of the same type run at the same time.
MAX_CONCURRENT_RUNS = 10

_MINUTE_MS = 60 * 1000
_MAX_LISTED_NAMES = 10


@dataclass(frozen=True)
class _Run:
    """A run of a compute resource. The key links the run to the schedule that started it, if any."""

    name: str
    start: int
    end: int
    finished: bool
    key: str | None = None


@dataclass(frozen=True)
class _Schedule:
    key: str
    name: str
    cron_expression: str
    timezone: str | None = None


@dataclass
class _Compute:
    """The schedules of one type of compute, and its runs within the lookback period."""

    kind: str
    url: str
    schedules: list[_Schedule] = field(default_factory=list)
    runs: list[_Run] = field(default_factory=list)


@dataclass(frozen=True)
class _Peak:
    count: int
    names: list[str]


class PerformanceRuleSet(ToolkitRuleSet):
    """Checks for thundering herds: many transformations, functions, or workflows running at the same time.

    Two sources are used. Looking forward, the schedules of each type of compute are expanded to the minutes they
    run in within the horizon from now, using the median run time of each schedule within the lookback period.
    Looking back, the runs within the lookback period are checked for the most that actually ran at the same time.
    """

    DISPLAY_NAME = "Performance"
    RESOURCE_TYPES = frozenset(
        {
            TransformationIO.as_resource_type(),
            TransformationScheduleIO.as_resource_type(),
            FunctionIO.as_resource_type(),
            FunctionScheduleIO.as_resource_type(),
            WorkflowIO.as_resource_type(),
            WorkflowTriggerIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"cdf"})

    def __init__(
        self,
        client: ToolkitClient | None = None,
        lookback_period: timedelta = timedelta(days=7),
        horizon: timedelta = timedelta(days=31),
    ) -> None:
        super().__init__(client=client)
        self.lookback_period = lookback_period
        self.horizon = horizon

    def get_status(self, data: ValidationInput) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="skip",
                message="Cannot check for compute running at the same time without a ToolkitClient. "
                "Provide one to the rule set.",
            )
        return RuleSetStatus(
            code="ready",
            message="Checks for transformations, functions, and workflows running at the same time, based on "
            f"their schedules in the next {self.horizon.days} days and their runs in CDF within the last "
            f"{self.lookback_period.days} days.",
        )

    def validate(self, data: ValidationInput) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        now = datetime_to_ms(_utc_now())
        from_time = now - int(self.lookback_period.total_seconds() * 1000)
        loaders = (self._load_transformations, self._load_functions, self._load_workflows)
        for loader in loaders:
            try:
                compute = loader(self.client, from_time, now)
            except ToolkitAPIError as error:
                yield InternalValidatorException(
                    message=f"Failed to retrieve compute resources from CDF: {error}", source="Performance"
                )
                continue
            if scheduled := self._scheduled_peak(compute, now):
                yield self._scheduled_warning(compute, *scheduled)
            if observed := self._observed_peak(compute):
                yield self._observed_warning(compute, *observed)

    # 1. Load

    @staticmethod
    def _load_transformations(client: ToolkitClient, from_time: int, now: int) -> _Compute:
        compute = _Compute(kind="transformations", url="/transformations/")
        for schedule in client.transformations.schedules.list(limit=None):
            if schedule.is_paused:
                continue
            compute.schedules.append(
                _Schedule(key=schedule.external_id, name=f"'{schedule.external_id}'", cron_expression=schedule.interval)
            )
        # Jobs are returned newest first, so we stop paging once we reach a job older than the lookback period.
        for jobs in client.transformations.jobs.iterate(limit=None):
            recent_jobs = [job for job in jobs if job.created_time >= from_time]
            for job in recent_jobs:
                if job.started_time is None:
                    continue
                compute.runs.append(
                    _Run(
                        name=f"'{job.transformation_external_id}'",
                        start=job.started_time,
                        end=job.finished_time or now,
                        finished=job.finished_time is not None,
                        key=job.transformation_external_id,
                    )
                )
            if len(recent_jobs) < len(jobs):
                break
        return compute

    @staticmethod
    def _load_functions(client: ToolkitClient, from_time: int, now: int) -> _Compute:
        compute = _Compute(kind="functions", url="/functions/")
        functions = client.functions.list(limit=None)
        external_id_by_id = {function.id: function.external_id or function.name for function in functions}
        for schedule in client.functions.schedules.list(limit=None):
            if schedule.function_id is None:
                continue
            function_name = schedule.function_external_id or external_id_by_id.get(
                schedule.function_id, str(schedule.function_id)
            )
            compute.schedules.append(
                _Schedule(
                    key=str(schedule.id),
                    name=f"'{schedule.name}' of '{function_name}'",
                    cron_expression=schedule.cron_expression,
                )
            )
        for function in functions:
            if function.last_called is None or function.last_called < from_time:
                continue
            calls = client.functions.calls.list(
                function_id=function.id,
                filter=FunctionCallFilter(start_time=EpochTimestampRange(min=from_time)),
                limit=None,
            )
            compute.runs.extend(
                _Run(
                    name=f"'{external_id_by_id[function.id]}'",
                    start=call.start_time,
                    end=call.end_time or now,
                    finished=call.end_time is not None,
                    key=str(call.schedule_id) if call.schedule_id is not None else None,
                )
                for call in calls
            )
        return compute

    @staticmethod
    def _load_workflows(client: ToolkitClient, from_time: int, now: int) -> _Compute:
        compute = _Compute(kind="workflows", url="/workflows/")
        for trigger in client.workflows.triggers.list(limit=None):
            rule = trigger.trigger_rule
            if trigger.is_paused or not isinstance(rule, ScheduleTriggerRule):
                continue
            compute.schedules.append(
                _Schedule(
                    key=f"{trigger.workflow_external_id}:{trigger.workflow_version}",
                    name=f"'{trigger.external_id}'",
                    cron_expression=rule.cron_expression,
                    timezone=rule.timezone,
                )
            )
        executions = client.workflows.executions.list(
            filter=WorkflowExecutionFilter(created_time_start=from_time), limit=None
        )
        compute.runs.extend(
            _Run(
                name=f"'{execution.workflow_external_id}'",
                start=execution.start_time,
                end=execution.end_time or now,
                finished=execution.end_time is not None,
                key=f"{execution.workflow_external_id}:{execution.version}" if execution.version else None,
            )
            for execution in executions
            if execution.start_time is not None
        )
        return compute

    # 2. Find peaks

    def _scheduled_peak(self, compute: _Compute, now: int) -> tuple[_Peak, int, int] | None:
        """The most schedules running in the same minute within the horizon, the first time they do so, and the
        number of minutes within the horizon more than MAX_CONCURRENT_RUNS are running."""
        window_start = ms_to_datetime(now - now % _MINUTE_MS)
        horizon_minutes = int(self.horizon.total_seconds() // 60)
        window_end = window_start + timedelta(minutes=horizon_minutes)
        duration_by_key = _median_duration_minutes(compute.runs)
        running_count_by_minute = [0] * horizon_minutes
        running_minutes_by_schedule: list[tuple[_Schedule, set[int]]] = []
        for schedule in compute.schedules:
            duration = min(duration_by_key.get(schedule.key, 1), horizon_minutes)
            try:
                # Starting earlier includes the runs that are still running when the window starts.
                starts = cron_starts(
                    schedule.cron_expression, window_start - timedelta(minutes=duration), window_end, schedule.timezone
                )
            except ValueError:
                continue
            offsets = [int((start - window_start).total_seconds() // 60) for start in starts]
            running_minutes = _running_minutes(offsets, duration, horizon_minutes)
            for minute in running_minutes:
                running_count_by_minute[minute] += 1
            running_minutes_by_schedule.append((schedule, running_minutes))
        peak_count = max(running_count_by_minute, default=0)
        if peak_count <= MAX_CONCURRENT_RUNS:
            return None
        peak_minute = running_count_by_minute.index(peak_count)
        names = [schedule.name for schedule, minutes in running_minutes_by_schedule if peak_minute in minutes]
        congested_minutes = sum(1 for count in running_count_by_minute if count > MAX_CONCURRENT_RUNS)
        peak_time = datetime_to_ms(window_start) + peak_minute * _MINUTE_MS
        return _Peak(count=peak_count, names=names), peak_time, congested_minutes

    @staticmethod
    def _observed_peak(compute: _Compute) -> tuple[_Peak, int] | None:
        """The most runs that ran at the same time within the lookback period, and when it first happened."""
        # Ends sort before starts at the same time, such that back-to-back runs do not count as running together.
        events = sorted([(run.start, 1) for run in compute.runs] + [(run.end, -1) for run in compute.runs])
        running = peak_count = peak_time = 0
        for timestamp, change in events:
            running += change
            if running > peak_count:
                peak_count, peak_time = running, timestamp
        if peak_count <= MAX_CONCURRENT_RUNS:
            return None
        names = [run.name for run in compute.runs if run.start <= peak_time < run.end]
        return _Peak(count=peak_count, names=names), peak_time

    # 3. Report

    def _scheduled_warning(
        self, compute: _Compute, peak: _Peak, peak_time: int, congested_minutes: int
    ) -> BuildWarning:
        return BuildWarning(
            code="CONGESTED-SCHEDULE",
            message=(
                f"Up to {peak.count} {compute.kind} are scheduled to run at the same time, next at "
                f"{_format_time(peak_time)} UTC: {_humanize_names(peak.names)}. More than {MAX_CONCURRENT_RUNS} "
                f"run at the same time during {congested_minutes} minutes in the next {self.horizon.days} days, "
                f"based on their schedules and median run time in the last {self.lookback_period.days} days. "
                "Running many at once can exceed concurrency limits, which delays or fails runs."
            ),
            source_url=compute.url,
            fix=_fix(compute.kind),
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )

    def _observed_warning(self, compute: _Compute, peak: _Peak, peak_time: int) -> BuildWarning:
        return BuildWarning(
            code="CONGESTED-SCHEDULE",
            message=(
                f"{peak.count} runs of {compute.kind} were running at the same time at "
                f"{_format_time(peak_time)} UTC, the most in the last "
                f"{self.lookback_period.days} days: {_humanize_names(peak.names)}. Running many at once can exceed "
                "concurrency limits, which delays or fails runs."
            ),
            source_url=compute.url,
            fix=_fix(compute.kind),
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )


def _median_duration_minutes(runs: Sequence[_Run]) -> dict[str, int]:
    """The median run time, rounded up to whole minutes, of the finished runs started by each schedule."""
    durations_by_key: dict[str, list[int]] = defaultdict(list)
    for run in runs:
        if run.finished and run.key is not None:
            durations_by_key[run.key].append(run.end - run.start)
    return {
        key: max(1, math.ceil(statistics.median(durations) / _MINUTE_MS)) for key, durations in durations_by_key.items()
    }


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _running_minutes(start_offsets: Sequence[int], duration_minutes: int, horizon_minutes: int) -> set[int]:
    """The minutes within the horizon a schedule is running, given its sorted start offsets from the window start."""
    running: set[int] = set()
    covered_until = 0
    for offset in start_offsets:
        end = min(offset + duration_minutes, horizon_minutes)
        running.update(range(max(offset, covered_until), end))
        covered_until = max(covered_until, end)
    return running


def _format_time(timestamp: int) -> str:
    return ms_to_datetime(timestamp).strftime("%Y-%m-%d %H:%M")


def _humanize_names(names: Sequence[str]) -> str:
    unique = sorted(set(names))
    if len(unique) <= _MAX_LISTED_NAMES:
        return humanize_collection(unique)
    return f"{', '.join(unique[:_MAX_LISTED_NAMES])} and {len(unique) - _MAX_LISTED_NAMES} more"


def _fix(kind: str) -> str:
    return (
        f"Spread the start times of the {kind} over different minutes, for example, '7 * * * *' instead of "
        f"'0 * * * *', or run them in sequence in a workflow."
    )
