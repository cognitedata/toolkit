from typing import Annotated, Any, Optional

import typer
from rich import print
from rich.panel import Panel
from rich.prompt import Confirm, Prompt


class InteractiveInit(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.interactive)

    def get_packages(self) -> dict[str, dict[str, Any]]:
        return {
            "quickstart": {
                "name": "Quick Start",
                "readme": "A set of modules for a CDF quick start project.",
                "content": [
                    {"data ingestion": ["sap", "pi", "mqtt"]},
                    {"data ingestion": ["sap", "pi", "mqtt"]},
                    {"data transformation": ["foo", "bar"]},
                    {"data contextualization": ["flash", "thunder"]},
                    {"infield": ["app"]},
                ],
            },
            "examples": {
                "name": "Examples",
                "readme": "This is a set of example modules for inspiration",
                "content": [
                    {"cdf_data_pipeline_asset_valhall": []},
                    {"cdf_data_pipeline_files_valhall": []},
                ],
            },
            "reference": {
                "readme": "All supported resources as reference",
                "content": [{"workflow": []}, {"transformations": []}, {"functions": []}, {"groups": []}],
            },
        }

    def interactive(
        self,
        ctx: typer.Context,
        init_dir: Annotated[
            Optional[str],
            typer.Option(
                help="Directory path to project to initialize or upgrade with templates.",
            ),
        ] = None,
        arg_selected: Annotated[
            Optional[str],
            typer.Option(
                help="List of modules to include. Options are 'get_content()'",
            ),
        ] = None,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        print(Panel("Initializing or upgrading a new CDF project with templates interactively."))

        if not init_dir:
            init_dir = Prompt.ask("[bold]Which directory would you like to use?[/] :sunglasses:", default="new_project")
        else:
            print(f"Using directory {init_dir}")

        if not arg_selected:
            selected: dict[str, list[str]] = dict()

            print("\n[bold]Which modules would you like to include?[/] :rocket:")
            total_items = len(self.get_packages())
            for i, (package_id, package_content) in enumerate(self.get_packages().items(), start=1):
                if package_content is None:
                    package_content = {}
                if Confirm.ask(
                    f"\n    [bold green]{i}/{total_items}: {package_content.get('name', package_id)}[/]: {package_content.get('readme', '')}"
                ):
                    subpackages = package_content.get("content", [])
                    if len(subpackages) == 0:
                        selected[package_id] = subpackages
                        continue
                    elif len(subpackages) == 1:
                        selected[package_id] = [subpackages[0]]
                    else:
                        if Confirm.ask(
                            f"        [green]The package contains {len(subpackages)} modules. Would you like to include {'both modules' if len(subpackages) == 2 else 'all of them'}?[/]",
                            default=True,
                        ):
                            selected[package_id] = subpackages
                        else:
                            for j, subpackage in enumerate(subpackages, start=1):
                                selected[package_id] = []
                                if Confirm.ask(
                                    f"        [green]{j}/{len(subpackages)}Would you like to include {subpackage} ?[/]",
                                    default=True,
                                ):
                                    selected[package_id].append(subpackage)

                    # selected = package
                    # break
                # include = Confirm.ask(f"[bold]Would you like to include {package} modules?[/]", default=True):
                # if isinstance(module, dict):
                #     print(f"- {package}")
                #     for sub_package, sub_module in module.items():
                #         print(f"  - {sub_package}")
                #         for sub_sub_module in sub_module:
                #             print(f"    - {sub_sub_module}")
                # print(f"  - {module}")

        print("You have selected...")
        print(selected)

        if not Confirm.ask("[bold]Would you like to continue?[/bold]", default=True):
            print("Exiting...")
            raise typer.Exit()


command = InteractiveInit(
    name="init", help="Initialize or upgrade a new CDF project with templates interactively."
).interactive
