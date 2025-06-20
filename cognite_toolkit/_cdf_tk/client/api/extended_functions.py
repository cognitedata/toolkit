from cognite.client._api.functions import FunctionsAPI, FunctionSchedulesAPI
from cognite.client._cognite_client import ClientConfig, CogniteClient
from cognite.client.data_classes import FunctionSchedule, FunctionScheduleWrite


class ExtendedFunctionsAPI(FunctionsAPI):
    """This class extends the FunctionsAPI to include additional functionality."""

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient):
        super().__init__(config, api_version, cognite_client)
        self.schedules: ExtendedFunctionSchedulesAPI = ExtendedFunctionSchedulesAPI(config, api_version, cognite_client)


class ExtendedFunctionSchedulesAPI(FunctionSchedulesAPI):
    """This class extends the FunctionSchedulesAPI to include additional functionality."""

    def create_api(self, item: FunctionScheduleWrite, nonce: str) -> FunctionSchedule:
        """Create a schedule associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/postFunctionSchedules>`_

        The difference between this method and the original create method is
        1. It allows you to pass the nonce directly.
        2. It does not mutate the FunctionScheduleWrite object and always use the functionId instead of the functionExternalId.

        Args:
            item (FunctionScheduleWrite): The schedule to create.
            nonce (str): Nonce retrieved from sessions API when creating a session. This will be used to bind the
                session before executing the function. The corresponding access token will be passed to the
                function and used to instantiate the client of the handle() function. You can create a session
                via the Sessions API.

        Returns:
            FunctionSchedule: The created schedule.

        """
        body = item.dump(camel_case=True)
        body["nonce"] = nonce

        response = self._post(
            url_path="/functions/schedules",
            json=body,
        )
        return FunctionSchedule._load(response.json(), cognite_client=self._cognite_client)
