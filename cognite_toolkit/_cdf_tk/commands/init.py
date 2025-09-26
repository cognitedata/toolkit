from rich import print
from rich.panel import Panel

from ._base import ToolkitCommand


class InitCommand(ToolkitCommand):
    def execute(self) -> None:
        InitCommand.poster()

    @staticmethod
    def poster() -> None:
        print(
            Panel(
                "\n".join(
                    [
                        "The Cognite Toolkit supports configuration of CDF projects from the command line or in CI/CD pipelines.",
                        "",
                        "[bold]Setup:[/]",
                        "1. Run [underline]cdf repo init[/] [italic]<directory name>[/] to set up a work directory.",
                        "2. Run [underline]cdf modules init[/] [italic]<directory name>[/] to initialise configuration modules.",
                        "",
                        "[bold]Configuration steps:[/]",
                        "3. Run [underline]cdf build[/] [italic]<directory name>[/] to verify the configuration for your project. Repeat for as many times as needed.",
                        "   Tip:[underline]cdf modules list[/] [italic]<directory name>[/] gives an overview of all your modules and their status.",
                        "",
                        "[bold]Deployment steps:[/]",
                        "4. Commit the [italic]<directory name>[/] to version control",
                        "5. Run [underline]cdf auth verify[/] to check that you have access to the relevant CDF project. ",
                        "    or [underline]cdf auth verify[/] if you have a .env file",
                        "6. Run [underline]cdf deploy --dry-run[/] to simulate the deployment of the configuration to the CDF project. Review the report provided.",
                        "7. Run [underline]cdf deploy[/] to deploy the configuration to the CDF project.",
                    ]
                ),
                title="Getting started",
                style="green",
                padding=(1, 2),
            )
        )
       