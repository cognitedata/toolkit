import shutil
import tempfile
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import questionary
import typer
from questionary import Choice
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.auth import AuthCommand
from cognite_toolkit._cdf_tk.commands.collect import CollectCommand
from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.commands.repo import RepoCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.feature_flags import Flags


class InitItemStatus(Enum):
    """Status of an init checklist item"""

    NONE = "none"
    SUCCESSFUL = "successful"
    FAILED = "failed"


@dataclass
class InitChecklistItem:
    """Represents an item in the init checklist"""

    name: str
    description: str
    function: Callable[[], None]
    status: InitItemStatus | None = None
    mandatory: bool = False

    def get_status_display(self) -> str:
        """Get a display string for the status"""
        if self.status == InitItemStatus.SUCCESSFUL:
            return "✓"
        elif self.status == InitItemStatus.FAILED:
            return "✗"
        else:
            return "○"

    def get_choice_title(self) -> str:
        """Get the title for the questionary choice"""
        status_icon = self.get_status_display()
        return f"{status_icon} {self.description} (required)" if self.mandatory else f"{status_icon} {self.description}"


class InitCommand(ToolkitCommand):
    def execute(self, dry_run: bool = False, emulate_dot_seven: bool = False) -> None:
        if not Flags.v07.is_enabled() and not emulate_dot_seven:
            print("This command is deprecated. Use 'cdf modules init' instead.")
            return

        print("\n")
        print(
            Panel(
                "Go through the following steps to set up the Cognite Toolkit. You can re-run steps if you need to change something.",
                title="Cognite Toolkit Setup",
                style="green",
                padding=(1, 2),
            )
        )

        # Initialize checklist items
        checklist_items = [
            InitChecklistItem(
                name="initToml",
                description="Create toml file",
                function=lambda: self._init_toml(dry_run=dry_run),
                mandatory=True,
            ),
            InitChecklistItem(
                name="initAuth",
                description="Authentication",
                function=lambda: self._init_auth(dry_run=dry_run),
            ),
            InitChecklistItem(
                name="initModules",
                description="Modules",
                function=lambda: self._init_modules(dry_run=dry_run),
            ),
            InitChecklistItem(
                name="initRepo",
                description="Git repository",
                function=lambda: self._init_repo(dry_run=dry_run),
            ),
            InitChecklistItem(
                name="initDataCollection",
                description="Usage statistics",
                function=lambda: self._init_data_collection(dry_run=dry_run),
            ),
        ]

        if CDFToml.load().is_loaded_from_file:
            checklist_items[0].status = InitItemStatus.SUCCESSFUL
            print("cdf.toml configuration file already exists. Skipping creation.")

        # Main loop: keep showing checklist until user is done
        while True:
            # Build choices for questionary
            choices = []
            for item in checklist_items:
                choices.append(
                    Choice(
                        title=item.get_choice_title(),
                        value=item.name,
                    )
                )
            # Check if all mandatory items are completed
            mandatory_items = [item for item in checklist_items if item.mandatory]
            all_mandatory_complete = all(item.status == InitItemStatus.SUCCESSFUL for item in mandatory_items)

            choices.append(Choice(title="> Quit", value="__exit__"))

            # Find the first item with status None to use as default
            default_item = next((item for item in checklist_items if item.status is None), None)
            default_value = default_item.name if default_item else "__exit__"

            # Show checklist and get user selection
            selected = questionary.select(
                "Select a task:",
                choices=choices,
                default=default_value,
            ).ask()

            # User cancelled (Ctrl+C or similar)
            if selected is None:
                return

            if selected == "__exit__":
                if all_mandatory_complete:
                    print("Setup complete!")
                    print("You can now start using the Cognite Toolkit.")
                    break
                else:
                    incomplete_mandatory = [
                        item.description for item in mandatory_items if item.status != InitItemStatus.SUCCESSFUL
                    ]
                    print(f"Warning: recommended item not completed: {', '.join(incomplete_mandatory)}")
                    break

            # Find the selected item
            selected_item = next((item for item in checklist_items if item.name == selected), None)
            if selected_item is None:
                continue

            # If item was already run, ask for confirmation to re-run
            if selected_item.status is not None:
                status_text = (
                    "successfully. Re-run it?"
                    if selected_item.status == InitItemStatus.SUCCESSFUL
                    else "with failure. Retry?"
                )
                confirm = questionary.confirm(
                    f"'{selected_item.description}' was already run {status_text}",
                    default=False,
                ).ask()
                if not confirm:
                    continue

            # Run the function
            try:
                selected_item.function()
                selected_item.status = InitItemStatus.SUCCESSFUL
                print(f"✓ {selected_item.description} completed successfully")
            except typer.Exit:
                # typer.Exit is used for normal exits, treat as success
                selected_item.status = InitItemStatus.SUCCESSFUL
                print(f"✓ {selected_item.description} completed successfully")
            except ToolkitError as e:
                # Catch expected toolkit errors
                selected_item.status = InitItemStatus.FAILED
                print(f"✗ {selected_item.description} failed: {e}")
            except Exception as e:
                # Catch unexpected errors and log full traceback for debugging
                selected_item.status = InitItemStatus.FAILED
                print(f"✗ {selected_item.description} failed: {e}")
                print(f"Unexpected error occurred. Full traceback:\n{traceback.format_exc()}")

    def _init_toml(self, dry_run: bool = False) -> None:
        organization_dir = ModulesCommand._prompt_organization_dir()
        if dry_run:
            print("Would initialize cdf.toml configuration file")
            return
        CDFToml.write(organization_dir, "dev")
        print(f"cdf.toml configuration file initialized in {organization_dir}")

    def _init_auth(self, dry_run: bool = False) -> None:
        auth_command = AuthCommand()
        auth_command.run(lambda: auth_command.init(no_verify=True, dry_run=dry_run))

    def _init_modules(self, dry_run: bool = False) -> None:
        with ModulesCommand() as modules_command:
            if dry_run:
                organization_dir = Path(tempfile.mkdtemp(prefix="init_modules_", suffix=".tmp", dir=Path.cwd()))
                modules_command.run(lambda: modules_command.init(organization_dir=organization_dir))
                shutil.rmtree(organization_dir)
            else:
                modules_command.run(lambda: modules_command.init())

    def _init_repo(self, dry_run: bool = False) -> None:
        repo_command = RepoCommand()
        repo_command.run(lambda: repo_command.init(cwd=Path.cwd(), host=None, verbose=False))

    def _init_data_collection(self, dry_run: bool = False) -> None:
        """Opt in to collect usage statistics"""

        opt_in = questionary.confirm(
            "Do you want to opt in to collect usage statistics?",
            default=True,
        ).ask()
        if dry_run:
            print("Would opt in to collect data" if opt_in else "Would not opt in to collect data")
            return

        if opt_in:
            collect_command = CollectCommand()
            collect_command.run(lambda: collect_command.execute("opt-in"))
        else:
            collect_command = CollectCommand()
            collect_command.run(lambda: collect_command.execute("opt-out"))
