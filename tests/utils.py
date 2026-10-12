from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.commands import DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import ReadResource
from cognite_toolkit._cdf_tk.resource_ios import (
    ResourceIO,
)


def to_deploy_status(definition_yaml: str | Path | MagicMock, loader: ResourceIO) -> dict[str, int]:
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
    resource_id = loader.get_id(resource)
    retrieved = loader.retrieve([resource_id])
    if not retrieved:
        created_ids = loader.create([resource])
        retrieved = loader.retrieve(list(created_ids))
    if not retrieved:
        raise RuntimeError(f"{type(loader).__name__} did not return a resource for {resource_id}")
    existing = retrieved[0]

    result = DeployV2Command.categorize_resources(
        loader,
        resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [filepath])},
        cdf_by_id={resource_id: existing},
    )
    return {
        "create": len(result.to_create),
        "change": len(result.to_update),
        "delete": len(result.to_delete),
        "unchanged": len(result.unchanged),
    }
