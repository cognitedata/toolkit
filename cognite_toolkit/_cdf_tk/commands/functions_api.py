from rich.json import JSON

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse, FunctionsActivation
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError


class FunctionsAPICommand(ToolkitCommand):
    """Commands for calling the Cognite Functions API directly."""

    def retrieve(self, client: ToolkitClient, external_id: str) -> FunctionResponse:
        """Retrieve a function, including its deployment status.

        Args:
            client: Toolkit client used to call CDF.
            external_id: External ID of the function.

        Returns:
            The function as returned by CDF.
        """
        function = self._get_function(client, external_id)
        client.console.print(JSON.from_data(function.dump()))
        return function

    def status(self, client: ToolkitClient) -> FunctionsActivation:
        """Get the Cognite Functions activation status for the project.

        Args:
            client: Toolkit client used to call CDF.

        Returns:
            Whether Functions are inactive, requested, or activated.
        """
        activation = client.functions.status()
        client.console.print(JSON.from_data(activation.dump()))
        return activation

    @staticmethod
    def _get_function(client: ToolkitClient, external_id: str) -> FunctionResponse:
        functions = client.functions.retrieve([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
        if not functions:
            raise ToolkitMissingResourceError(f"Function with external ID {external_id!r} does not exist.")
        return functions[0]
