from pathlib import Path

from cognite.client.data_classes import FunctionSchedule, FunctionScheduleWrite, FunctionScheduleWriteList

from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.loaders import (
    FunctionScheduleLoader,
)
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables


def modify_function_schedule_loader() -> None:
    split_character = "/\\"

    def get_id(cls: type[FunctionScheduleLoader], item: FunctionScheduleWrite | FunctionSchedule | dict) -> str:
        nonlocal split_character
        if isinstance(item, dict):
            if missing := tuple(k for k in {"functionExternalId", "name"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return f"{item['functionExternalId']}{split_character}{item['name']}"

        if item.function_external_id is None or item.name is None:
            raise ToolkitRequiredValueError("FunctionSchedule must have functionExternalId and Name set.")
        return f"{item.function_external_id}{split_character}{item.name}"

    FunctionScheduleLoader.get_id = classmethod(get_id)  # type: ignore[method-assign, assignment, arg-type]

    def load_resource(
        self: FunctionScheduleLoader, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionScheduleWrite | FunctionScheduleWriteList | None:
        nonlocal split_character
        schedules = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(schedules, dict):
            schedules = [schedules]

        for sched in schedules:
            ext_id = f"{sched['functionExternalId']}{split_character}{sched['name']}"
            if self.extra_configs.get(ext_id) is None:
                self.extra_configs[ext_id] = {}
            self.extra_configs[ext_id]["authentication"] = sched.pop("authentication", {})
        return FunctionScheduleWriteList.load(schedules)

    FunctionScheduleLoader.load_resource = load_resource  # type: ignore[method-assign]

    #
    # def _resolve_functions_ext_id(self, items: FunctionScheduleWriteList) -> FunctionScheduleWriteList:
    #     functions = FunctionLoader(self.client, None).retrieve(list(set([item.function_external_id for item in items])))
    #     for item in items:
    #         for func in functions:
    #             if func.external_id == item.function_external_id:
    #                 item.function_id = func.id  # type: ignore[assignment]
    #     return items

    # def retrieve(self, ids: SequenceNotStr[str]) -> FunctionSchedulesList:
    #     crons_by_function: dict[str, set[str]] = defaultdict(set)
    #     for id_ in ids:
    #         function_external_id, cron = id_.rsplit(self._split_character, 1)
    #         crons_by_function[function_external_id].add(cron)
    #     functions = FunctionLoader(self.client, None).retrieve(list(crons_by_function))
    #     schedules = FunctionSchedulesList([])
    #     for func in functions:
    #         ret = self.client.functions.schedules.list(function_id=func.id, limit=-1)
    #         for schedule in ret:
    #             schedule.function_external_id = func.external_id
    #         schedules.extend(
    #             [
    #                 schedule
    #                 for schedule in ret
    #                 if schedule.cron_expression in crons_by_function[cast(str, func.external_id)]
    #             ]
    #         )
    #     return schedules
    #
    # def create(self, items: FunctionScheduleWriteList) -> FunctionSchedulesList:
    #     items = self._resolve_functions_ext_id(items)
    #     created = []
    #     for item in items:
    #         key = f"{item.function_external_id}:{item.cron_expression}"
    #         auth_config = self.extra_configs.get(key, {}).get("authentication", {})
    #         if "clientId" in auth_config and "clientSecret" in auth_config:
    #             client_credentials = ClientCredentials(auth_config["clientId"], auth_config["clientSecret"])
    #         else:
    #             client_credentials = None
    #
    #         created.append(
    #             self.client.functions.schedules.create(
    #                 name=item.name or "",
    #                 description=item.description or "",
    #                 cron_expression=cast(str, item.cron_expression),
    #                 function_id=cast(int, item.function_id),
    #                 data=item.data,
    #                 client_credentials=client_credentials,
    #             )
    #         )
    #     return FunctionSchedulesList(created)
