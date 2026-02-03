import pytest
from cognite.client.data_classes import DataSet
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import HTTPResult, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulatorModelRevisionFilter
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import SimulatorModelRequest
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model_revision import SimulatorModelRevisionRequest
from tests_smoke.exceptions import EndpointAssertionError

SIMULATOR_EXTERNAL_ID = "smoke_test_simulator"
SIMULATOR_INTEGRATION_ID = "smoke_test_simulator_integration"


@pytest.fixture(scope="session")
def simulator(toolkit_client: ToolkitClient) -> str:
    """Create a simulator for testing, if it does not already exist.

    Note that these are unofficial API endpoints and may change without notice.
    """
    http_client = toolkit_client.http_client
    config = toolkit_client.config
    # Check if simulator already exists
    list_response = http_client.request_single_retries(
        RequestMessage(
            endpoint_url=config.create_api_url("/simulators/list"),
            method="POST",
            body_content={"limit": 1000},
        )
    )
    if simulator_external_id := _parse_simulator_response(list_response):
        return simulator_external_id

    creation_response = http_client.request_single_retries(
        RequestMessage(
            endpoint_url=config.create_api_url("/simulators"),
            method="POST",
            body_content={"items": [SIMULATOR]},
        )
    )
    simulator_external_id = _parse_simulator_response(creation_response)
    if simulator_external_id is not None:
        return simulator_external_id
    raise EndpointAssertionError("/simulators", "Failed to create simulator for testing.")


def _parse_simulator_response(list_response: HTTPResult) -> str | None:
    if not isinstance(list_response, SuccessResponse):
        raise EndpointAssertionError("/simulators/list", str(list_response))
    try:
        items = list_response.body_json["items"]
    except (KeyError, ValueError):
        raise AssertionError(f"Unexpected response format from /simulators/list: {list_response.body}") from None
    try:
        return next(item["externalId"] for item in items if item["externalId"] == SIMULATOR_EXTERNAL_ID)
    except (KeyError, StopIteration):
        return None


@pytest.fixture()
def simulator_model_revision_file(smoke_dataset: DataSet, toolkit_client: ToolkitClient) -> FileMetadataResponse:
    client = toolkit_client
    file = FileMetadataRequest(
        external_id="smoke_test_simulator_model_revision_file",
        name="simulator_model_revision.txt",
        directory="/files",
        data_set_id=smoke_dataset.id,
        mime_type="application/octet-stream",
    )

    existing = client.tool.filemetadata.retrieve([file.as_id()], ignore_unknown_ids=True)
    if existing:
        return existing[0]
    created = client.tool.filemetadata.create([file])
    if len(created) != 1:
        raise EndpointAssertionError(
            "/filemetadata",
            f"Expected 1 created file metadata, got {len(created)}",
        )
    if created[0].upload_url is None:
        raise EndpointAssertionError(
            "/filemetadata",
            f"Created file metadata {created[0].external_id} has no upload URL.",
        )
    content = b"This is a smoke test simulator model revision file."
    uploaded = client.http_client.request_single_retries(
        RequestMessage(
            endpoint_url=created[0].upload_url,
            method="PUT",
            content_type="application/octet-stream",
            data_content=content,
        )
    )
    if not isinstance(uploaded, SuccessResponse):
        raise EndpointAssertionError(
            "/filemetadata/upload",
            f"Failed to upload content for file metadata {created[0].external_id}: {uploaded}",
        )
    return created[0]


class TestSimulatorModelsAPI:
    def test_crudl(
        self,
        simulator: str,
        smoke_dataset: DataSet,
        toolkit_client: ToolkitClient,
        simulator_model_revision_file: FileMetadataResponse,
    ) -> None:
        model = SimulatorModelRequest(
            external_id="smoke_test_simulator_model",
            simulator_external_id=SIMULATOR_EXTERNAL_ID,
            name="Smoke Test Simulator Model",
            data_set_id=smoke_dataset.id,
            type="SteadyState",
        )
        revision = SimulatorModelRevisionRequest(
            external_id="smoke_test_simulator_revision",
            model_external_id=model.external_id,
            description="Smoke Test Simulator Revision",
            file_id=simulator_model_revision_file.id,
        )
        try:
            model_endpoints = toolkit_client.tool.simulators.models._method_endpoint_map
            created = toolkit_client.tool.simulators.models.create([model])
            if len(created) != 1:
                raise EndpointAssertionError(
                    model_endpoints["create"].path,
                    f"Expected 1 created simulator model, got {len(created)}",
                )
            if created[0].external_id != model.external_id:
                raise EndpointAssertionError(
                    model_endpoints["create"].path,
                    f"Expected created simulator model external ID to be {model.external_id}, got {created[0].external_id}",
                )

            retrieved = toolkit_client.tool.simulators.models.retrieve([model.as_id()])
            if len(retrieved) != 1:
                raise EndpointAssertionError(
                    model_endpoints["retrieve"].path,
                    f"Expected 1 retrieved simulator model, got {len(retrieved)}",
                )
            if retrieved[0].external_id != model.external_id:
                raise EndpointAssertionError(
                    model_endpoints["retrieve"].path,
                    f"Expected retrieved simulator model external ID to be {model.external_id}, got {retrieved[0].external_id}",
                )

            update_request = created[0].as_request_resource().model_copy(update={"description": "Updated description"})
            updated = toolkit_client.tool.simulators.models.update([update_request])
            if len(updated) != 1:
                raise EndpointAssertionError(
                    model_endpoints["update"].path,
                    f"Expected 1 updated simulator model, got {len(updated)}",
                )
            if updated[0].description != "Updated description":
                raise EndpointAssertionError(
                    model_endpoints["update"].path,
                    f"Expected updated simulator model description to be 'Updated description', got {updated[0].description}",
                )

            listed = toolkit_client.tool.simulators.models.list(limit=1)
            if len(listed) != 1:
                raise EndpointAssertionError(
                    model_endpoints["list"].path,
                    f"Expected 1 listed simulator model, got {len(listed)}",
                )

            revision_endpoints = toolkit_client.tool.simulators.model_revisions._method_endpoint_map
            created_revision = toolkit_client.tool.simulators.model_revisions.create([revision])
            if len(created_revision) != 1:
                raise EndpointAssertionError(
                    revision_endpoints["create"].path,
                    f"Expected 1 created simulator model revision, got {len(created_revision)}",
                )
            if created_revision[0].external_id != revision.external_id:
                raise EndpointAssertionError(
                    revision_endpoints["create"].path,
                    f"Expected created simulator model revision external ID to be {revision.external_id}, got {created_revision[0].external_id}",
                )

            retrieved_revision = toolkit_client.tool.simulators.model_revisions.retrieve([revision.as_id()])
            if len(retrieved_revision) != 1:
                raise EndpointAssertionError(
                    revision_endpoints["retrieve"].path,
                    f"Expected 1 retrieved simulator model revision, got {len(retrieved_revision)}",
                )
            if retrieved_revision[0].external_id != revision.external_id:
                raise EndpointAssertionError(
                    revision_endpoints["retrieve"].path,
                    f"Expected retrieved simulator model revision external ID to be {revision.external_id}, got {retrieved_revision[0].external_id}",
                )

            listed_revisions = list(
                toolkit_client.tool.simulators.model_revisions.iterate(
                    filter=SimulatorModelRevisionFilter(model_external_ids=[model.external_id]), limit=1
                )
            )
            if len(listed_revisions) != 1:
                raise EndpointAssertionError(
                    revision_endpoints["list"].path,
                    f"Expected 1 listed simulator model revision, got {len(listed_revisions)}",
                )
        finally:
            toolkit_client.tool.simulators.models.delete([model.as_id()])


SIMULATOR: dict[str, JsonValue] = {
    "name": "Smoke Test Simulator",
    "externalId": SIMULATOR_EXTERNAL_ID,
    "fileExtensionTypes": ["txt"],
    "modelTypes": [{"name": "Steady State", "key": "SteadyState"}],
    "modelDependencies": [
        {
            "fileExtensionTypes": ["txt", "xml"],
            "fields": [
                {"name": "fieldA", "label": "label fieldA", "info": "info fieldA"},
                {"name": "fieldB", "label": "label fieldB", "info": "info fieldB"},
            ],
        },
    ],
    "stepFields": [
        {
            "stepType": "get/set",
            "fields": [
                {
                    "name": "objectName",
                    "label": "Simulation Object Name",
                    "info": "Enter the name of the DWSIM object, i.e. Feed",
                },
                {
                    "name": "objectProperty",
                    "label": "Simulation Object Property",
                    "info": "Enter the property of the DWSIM object, i.e. Temperature",
                },
            ],
        },
        {
            "stepType": "command",
            "fields": [
                {
                    "name": "command",
                    "label": "Command",
                    "info": "Select a command",
                    "options": [{"label": "Solve Flowsheet", "value": "Solve"}],
                }
            ],
        },
    ],
    "unitQuantities": [
        {
            "name": "mass",
            "label": "Mass",
            "units": [{"label": "kg", "name": "kg"}, {"label": "g", "name": "g"}, {"label": "lb", "name": "lb"}],
        },
        {
            "name": "time",
            "label": "Time",
            "units": [{"label": "s", "name": "s"}, {"label": "min.", "name": "min."}, {"label": "h", "name": "h"}],
        },
        {
            "name": "accel",
            "label": "Acceleration",
            "units": [
                {"label": "m/s2", "name": "m/s2"},
                {"label": "cm/s2", "name": "cm/s2"},
                {"label": "ft/s2", "name": "ft/s2"},
            ],
        },
        {
            "name": "force",
            "label": "Force",
            "units": [
                {"label": "N", "name": "N"},
                {"label": "dyn", "name": "dyn"},
                {"label": "kgf", "name": "kgf"},
                {"label": "lbf", "name": "lbf"},
            ],
        },
        {
            "name": "volume",
            "label": "Volume",
            "units": [
                {"label": "m3", "name": "m3"},
                {"label": "cm3", "name": "cm3"},
                {"label": "L", "name": "L"},
                {"label": "ft3", "name": "ft3"},
                {"label": "bbl", "name": "bbl"},
                {"label": "gal[US]", "name": "gal[US]"},
                {"label": "gal[UK]", "name": "gal[UK]"},
            ],
        },
        {
            "name": "density",
            "label": "Density",
            "units": [
                {"label": "kg/m3", "name": "kg/m3"},
                {"label": "g/cm3", "name": "g/cm3"},
                {"label": "lbm/ft3", "name": "lbm/ft3"},
            ],
        },
        {
            "name": "diameter",
            "label": "Diameter",
            "units": [{"label": "mm", "name": "mm"}, {"label": "in", "name": "in"}],
        },
        {
            "name": "distance",
            "label": "Distance",
            "units": [{"label": "m", "name": "m"}, {"label": "ft", "name": "ft"}, {"label": "cm", "name": "cm"}],
        },
        {
            "name": "heatflow",
            "label": "Heat Flow",
            "units": [
                {"label": "kW", "name": "kW"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "BTU/h", "name": "BTU/h"},
                {"label": "BTU/s", "name": "BTU/s"},
                {"label": "cal/s", "name": "cal/s"},
                {"label": "HP", "name": "HP"},
                {"label": "kJ/h", "name": "kJ/h"},
                {"label": "kJ/d", "name": "kJ/d"},
                {"label": "MW", "name": "MW"},
                {"label": "W", "name": "W"},
                {"label": "BTU/d", "name": "BTU/d"},
                {"label": "MMBTU/d", "name": "MMBTU/d"},
                {"label": "MMBTU/h", "name": "MMBTU/h"},
                {"label": "kcal/s", "name": "kcal/s"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "kcal/d", "name": "kcal/d"},
            ],
        },
        {
            "name": "pressure",
            "label": "Pressure",
            "units": [
                {"label": "Pa", "name": "Pa"},
                {"label": "atm", "name": "atm"},
                {"label": "kgf/cm2", "name": "kgf/cm2"},
                {"label": "kgf/cm2g", "name": "kgf/cm2g"},
                {"label": "lbf/ft2", "name": "lbf/ft2"},
                {"label": "kPa", "name": "kPa"},
                {"label": "kPag", "name": "kPag"},
                {"label": "bar", "name": "bar"},
                {"label": "barg", "name": "barg"},
                {"label": "ftH2O", "name": "ftH2O"},
                {"label": "inH2O", "name": "inH2O"},
                {"label": "inHg", "name": "inHg"},
                {"label": "mbar", "name": "mbar"},
                {"label": "mH2O", "name": "mH2O"},
                {"label": "mmH2O", "name": "mmH2O"},
                {"label": "mmHg", "name": "mmHg"},
                {"label": "MPa", "name": "MPa"},
                {"label": "psi", "name": "psi"},
                {"label": "psig", "name": "psig"},
            ],
        },
        {
            "name": "velocity",
            "label": "Velocity",
            "units": [
                {"label": "m/s", "name": "m/s"},
                {"label": "cm/s", "name": "cm/s"},
                {"label": "mm/s", "name": "mm/s"},
                {"label": "km/h", "name": "km/h"},
                {"label": "ft/h", "name": "ft/h"},
                {"label": "ft/min", "name": "ft/min"},
                {"label": "ft/s", "name": "ft/s"},
                {"label": "in/s", "name": "in/s"},
            ],
        },
        {
            "name": "temperature",
            "label": "Temperature",
            "units": [
                {"label": "K", "name": "K"},
                {"label": "R", "name": "R"},
                {"label": "C", "name": "C"},
                {"label": "F", "name": "F"},
            ],
        },
        {
            "name": "volumetricFlow",
            "label": "Volumetric Flow",
            "units": [
                {"label": "m3/h", "name": "m3/h"},
                {"label": "cm3/s", "name": "cm3/s"},
                {"label": "L/h", "name": "L/h"},
                {"label": "L/min", "name": "L/min"},
                {"label": "L/s", "name": "L/s"},
                {"label": "ft3/h", "name": "ft3/h"},
                {"label": "ft3/min", "name": "ft3/min"},
                {"label": "ft3/s", "name": "ft3/s"},
                {"label": "gal[US]/h", "name": "gal[US]/h"},
                {"label": "gal[US]/min", "name": "gal[US]/min"},
                {"label": "gal[US]/s", "name": "gal[US]/s"},
                {"label": "gal[UK]/h", "name": "gal[UK]/h"},
                {"label": "gal[UK]/min", "name": "gal[UK]/min"},
                {"label": "gal[UK]/s", "name": "gal[UK]/s"},
            ],
        },
    ],
}
