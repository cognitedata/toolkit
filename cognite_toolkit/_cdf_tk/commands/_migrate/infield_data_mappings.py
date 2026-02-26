from pathlib import Path

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file


def create_infield_data_mappings() -> dict[str, ViewToViewMapping]:
    mappings_path = Path(__file__).parent / "infield_data_mappings.yaml"
    mappings_dict = read_yaml_file(mappings_path, expected_output="dict")
    return TypeAdapter(dict[str, ViewToViewMapping]).validate_python(mappings_dict)
