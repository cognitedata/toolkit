import builtins
from typing import Any, Literal

from pydantic import JsonValue
from rich.json import JSON

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import InstanceResponse
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.cli_args import parse_view_str
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content


class InstancesAPICommand(ToolkitCommand):
    """Commands for working with data modeling instances."""

    def list(
        self,
        client: ToolkitClient,
        view: str,
        filter: str | None = None,
        limit: int = 25,
        instance_type: Literal["node", "edge"] = "node",
    ) -> builtins.list[InstanceResponse]:
        """List instances using the instances search endpoint.

        Args:
            client: Toolkit client used to call CDF.
            view: View given as 'space:externalId/version'. Properties from this view are returned.
            filter: Optional YAML/JSON filter expression.
            limit: Maximum number of instances to return.
            instance_type: Whether to list nodes or edges. Defaults to nodes.

        Returns:
            Matching instances from CDF.
        """
        view_id = parse_view_str(view)
        parsed_filter = self._parse_filter(filter)
        instances = client.tool.instances.search(
            view=view_id,
            filter=parsed_filter,
            limit=limit,
            instance_type=instance_type,
        )
        client.console.print(JSON.from_data([instance.dump() for instance in instances]))
        return instances

    @staticmethod
    def _parse_filter(filter: str | None) -> dict[str, JsonValue] | None:
        if filter is None:
            return None
        try:
            parsed: Any = read_yaml_content(filter)
        except Exception as e:
            raise ToolkitValueError(f"Invalid filter expression: {e}") from e
        if not isinstance(parsed, dict):
            raise ToolkitValueError("Filter expression must be a YAML/JSON object.")
        return parsed
