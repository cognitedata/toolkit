from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from cognite.client.data_classes import (
    ClientCredentials,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionScheduleWrite,
    FunctionScheduleWriteList,
)
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.loaders import (
    FunctionLoader,
    FunctionScheduleLoader,
)
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables


@dataclass(frozen=True)
class FunctionScheduleID:
    function_external_id: str
    name: str


def modify_function_schedule_loader() -> None:
    def get_id(
        cls: type[FunctionScheduleLoader], item: FunctionScheduleWrite | FunctionSchedule | dict
    ) -> FunctionScheduleID:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"functionExternalId", "name"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return FunctionScheduleID(item["functionExternalId"], item["name"])

        if item.function_external_id is None or item.name is None:
            raise ToolkitRequiredValueError("FunctionSchedule must have functionExternalId and Name set.")
        return FunctionScheduleID(item.function_external_id, item.name)

    FunctionScheduleLoader.get_id = classmethod(get_id)  # type: ignore[method-assign, assignment, arg-type]

    def load_resource(
        self: FunctionScheduleLoader, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionScheduleWrite | FunctionScheduleWriteList | None:
        schedules = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(schedules, dict):
            schedules = [schedules]

        for schedule in schedules:
            identifier = self.get_id(schedule)
            if self.extra_configs.get(identifier) is None:
                self.extra_configs[identifier] = {}
            self.extra_configs[identifier]["authentication"] = schedule.pop("authentication", {})
            if "functionId" in schedule:
                LowSeverityWarning(f"FunctionId will be ignored in the schedule {schedule.get('functionExternalId', 'Misssing')!r}").print_warning()
                schedule.pop("functionId", None)
        return FunctionScheduleWriteList.load(schedules)

    FunctionScheduleLoader.load_resource = load_resource  # type: ignore[method-assign]

    def retrieve(self: FunctionScheduleLoader, ids: SequenceNotStr[FunctionScheduleID]) -> FunctionSchedulesList:
        names_by_function: dict[str, set[str]] = defaultdict(set)
        for id_ in ids:
            names_by_function[id_.function_external_id].add(id_.name)
        functions = FunctionLoader(self.client, None).retrieve(list(names_by_function))
        schedules = FunctionSchedulesList([])
        for func in functions:
            ret = self.client.functions.schedules.list(function_id=func.id, limit=-1)
            for schedule in ret:
                schedule.function_external_id = func.external_id
            schedules.extend(
                [schedule for schedule in ret if schedule.name in names_by_function[cast(str, func.external_id)]]
            )
        return schedules

    FunctionScheduleLoader.retrieve = retrieve  # type: ignore[method-assign, assignment]

    def create(self: FunctionScheduleLoader, items: FunctionScheduleWriteList) -> FunctionSchedulesList:
        created = []
        for item in items:
            key = self.get_id(item)
            auth_config = self.extra_configs.get(key, {}).get("authentication", {})
            if "clientId" in auth_config and "clientSecret" in auth_config:
                client_credentials = ClientCredentials(auth_config["clientId"], auth_config["clientSecret"])
            else:
                client_credentials = None

            created.append(
                self.client.functions.schedules.create(
                    item,
                    client_credentials=client_credentials,
                )
            )
        return FunctionSchedulesList(created)

    FunctionScheduleLoader.create = create  # type: ignore[method-assign]
