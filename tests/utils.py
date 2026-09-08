from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.commands import DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import ReadResource
from cognite_toolkit._cdf_tk.resource_ios import (
    ResourceIO,
)


def to_deploy_status(definition_yaml: str | MagicMock, loader: ResourceIO) -> dict[str, int]:
    """This is a helper function to test that a YAML definiition of a resource is
    correctly categorized into create, change, delete, or unchanged."""
    if isinstance(definition_yaml, str):
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = definition_yaml
    else:
        filepath = definition_yaml

    resource_dict = loader.load_resource_file(filepath, {})
    assert len(resource_dict) == 1
    resource = loader.load_resource(deepcopy(resource_dict[0]))
    resource_id = resource.as_id()
    existing_list = loader.retrieve([resource_id])
    if not existing_list:
        existing_list = loader.create([resource])

    result = DeployV2Command.categorize_resources(
        loader,
        resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [filepath])},
        cdf_by_id={resource_id: existing_list[0]},
    )
    return {
        "create": len(result.to_create),
        "change": len(result.to_update),
        "delete": len(result.to_delete),
        "unchanged": len(result.unchanged),
    }
