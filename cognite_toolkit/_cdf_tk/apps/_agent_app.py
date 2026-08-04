from pathlib import Path
from typing import Annotated

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import AgentCommand
from cognite_toolkit._cdf_tk.commands.agent import DEFAULT_AGENT_EXTERNAL_ID, DEFAULT_AGENT_MODEL, PERMISSION_MODES

CDF_TOML = CDFToml.load(Path.cwd())


class AgentApp:
    """Holds the callback for the ``cdf .`` agent command."""

    @staticmethod
    def agent(
        ctx: typer.Context,
        prompt: Annotated[
            list[str] | None,
            typer.Argument(
                help="Your request in natural language. Everything after 'cdf .' that is not an "
                "option is treated as the prompt.",
            ),
        ] = None,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Path to the Toolkit project directory the agent should work in.",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        env_name: Annotated[
            str | None,
            typer.Option(
                "--env",
                "-e",
                help="The default build environment the agent should assume.",
            ),
        ] = CDF_TOML.cdf.default_env,
        agent_external_id: Annotated[
            str | None,
            typer.Option(
                "--agent",
                help=f"Existing agent external ID to use. Defaults to {DEFAULT_AGENT_EXTERNAL_ID!r}.",
            ),
        ] = None,
        model: Annotated[
            str,
            typer.Option(
                "--model",
                "-m",
                help="Model to use when provisioning the default agent.",
            ),
        ] = DEFAULT_AGENT_MODEL,
        permission_mode: Annotated[
            str,
            typer.Option(
                "--permission-mode",
                "-p",
                help=f"How local tool use is confirmed. One of: {', '.join(PERMISSION_MODES)}.",
            ),
        ] = "acceptEdits",
        max_steps: Annotated[
            int,
            typer.Option(
                "--max-steps",
                help="Maximum number of agent/tool iterations before stopping.",
            ),
        ] = 50,
        new_session: Annotated[
            bool,
            typer.Option(
                "--new",
                help="Start a new conversation instead of continuing the previous one.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Show tool input/output details.",
            ),
        ] = False,
    ) -> None:
        """Turn a natural-language prompt into Toolkit actions using a CDF agent session.

        Example: cdf . create a data model for weather stations
        """
        words = list(prompt or [])
        words.extend(ctx.args)
        prompt_text = " ".join(words).strip()
        if not prompt_text:
            print(
                "Usage: [bold yellow]cdf . <your request>[/]\n"
                "Example: [bold yellow]cdf . add a transformation that loads assets[/]"
            )
            raise typer.Exit()

        cmd = AgentCommand()
        cmd.run(
            lambda: cmd.execute(
                prompt_text,
                organization_dir,
                env_name,
                agent_external_id,
                model,
                permission_mode,
                max_steps,
                new_session,
                verbose,
            )
        )
