from rich import print
from rich.panel import Panel

from ._base import ToolkitCommand


class InitCommand(ToolkitCommand):
    def execute(self) -> None:
        print(
            Panel(
                "\n".join(
                    [
                        "[bold]Follow these steps to get started with the Cognite Data Fusion Toolkit:[/b]",
                        "",
                        "1. Run [bold]cdf-tk repo init (folder_name)[/] to prepare the working folder.",  # todo: connect the folder name with the modules command
                        "2. Run [bold]cd (folder_name)[/] to enter the newly created folder.",
                        "3. Run [bold]cdf-tk modules init[/] to select the configuration modules you would like to start with.",
                        "4. Run [bold]cdf-tk auth verify --interactive[/] to check that you have access to the relevant CDF project. ",
                        "   [italic](if you already have a .env file, you can copy it into the folder and run [bold]cdf-tk auth verify[/] directly)[/]",
                        "5. Run [bold]cdf-tk build modules[/] to build the configuration and look for variables that need your attention. Repeat for as many times as needed.",
                        "6. Run [bold]cdf-tk deploy --dry-run[/] to simulate the deployment of the configuration to the CDF project. Review the report provided",
                        "7. Run [bold]cdf-tk deploy[/] to deploy the configuration to the CDF project.",
                        "8. Commit the changes to your version control system.",
                    ]
                ),
                title="Getting started",
                style="green",
                padding=(1, 2),
            )
        )
