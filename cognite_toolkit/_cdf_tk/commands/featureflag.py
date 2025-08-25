from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags


class FeatureFlagCommand(ToolkitCommand):
    @staticmethod
    def list() -> None:
        user_settings = CDFToml.load().alpha_flags
        table = Table(title="feature flags")
        table.add_column("Name", justify="left")
        table.add_column("Description", justify="left")
        table.add_column("Status", justify="left")

        for flag in Flags:
            is_enabled = user_settings.get(flag.name, False)
            if is_enabled or flag.value.visible:
                table.add_row(
                    flag.name,
                    flag.value.description,
                    "enabled" if is_enabled else "disabled",
                    style="yellow" if is_enabled else "",
                )
        print(table)
