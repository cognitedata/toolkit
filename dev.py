"""This is a small CLI used to develop Toolkit."""

from datetime import date
from pathlib import Path

import typer
from packaging.version import Version, parse

from cognite_toolkit._version import __version__

REPO_ROOT = Path(__file__).parent
CHANGELOG = REPO_ROOT / "CHANGELOG.cdf-tk.md"
TEMPLATE_CHANGELOG = REPO_ROOT / "CHANGELOG.templates.md"
TBD_HEADING = "## TBD"

bump_app = typer.Typer(
    add_completion=False,
    help=__doc__,
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


@bump_app.command()
def bump(
    major: bool = False,
    minor: bool = False,
    patch: bool = False,
    alpha: bool = False,
    beta: bool = False,
    verbose: bool = False,
) -> None:
    version_files = [
        REPO_ROOT / "pyproject.toml",
        REPO_ROOT / "cognite_toolkit" / "_version.py",
        REPO_ROOT / "cdf.toml",
        *(REPO_ROOT / "tests" / "data").rglob("cdf.toml"),
        *(REPO_ROOT / "tests" / "data").rglob("_build_environment.yaml"),
        *(REPO_ROOT / "cognite_toolkit").rglob("cdf.toml"),
    ]
    version = parse(__version__)

    if alpha and version.is_prerelease and version.pre[0] == "a":
        suffix = f"a{version.pre[1] + 1}"
    elif alpha and version.is_prerelease and version.pre[0] == "b":
        raise typer.BadParameter("Cannot bump to alpha version when current version is a beta prerelease.")
    elif alpha and not version.is_prerelease:
        suffix = "a1"
    elif beta and version.is_prerelease and version.pre[0] == "a":
        suffix = "b1"
    elif beta and version.is_prerelease and version.pre[0] == "b":
        suffix = f"b{version.pre[1] + 1}"
    elif beta and not version.is_prerelease:
        raise typer.BadParameter("Cannot bump to beta version when current version is not an alpha prerelease.")
    else:
        suffix = ""

    if major:
        new_version = Version(f"{version.major + 1}.0.0{suffix}")
    elif minor:
        new_version = Version(f"{version.major}.{version.minor + 1}.0{suffix}")
    elif patch:
        new_version = Version(f"{version.major}.{version.minor}.{version.micro + 1}{suffix}")
    elif alpha or beta:
        new_version = Version(f"{version.major}.{version.minor}.{version.micro}{suffix}")
    else:
        raise typer.BadParameter("You must specify one of major, minor, patch, alpha, or beta.")

    # Update Changelog
    changelog = CHANGELOG.read_text()
    template_changelog = TEMPLATE_CHANGELOG.read_text()
    if TBD_HEADING not in changelog and TBD_HEADING not in template_changelog:
        raise ValueError(
            f"There are no changes to release. The changelog does not contain a TBD section: {TBD_HEADING}."
        )

    today = date.today().strftime("%Y-%m-%d")
    new_heading = f"## [{new_version}] - {today}"

    for content, file, name in [
        (changelog, CHANGELOG, "cdf CLI"),
        (template_changelog, TEMPLATE_CHANGELOG, "templates"),
    ]:
        if TBD_HEADING in content:
            content = content.replace(TBD_HEADING, new_heading)
            file.write_text(content)
            if verbose:
                typer.echo(f"Updated {file.name!r} changelog with new heading: {new_heading}.")
        else:
            new_changelog: list[str] = []
            has_added_entry = False
            for line in content.splitlines():
                if not has_added_entry and line.startswith("##"):
                    new_changelog.append(new_heading)
                    new_changelog.append("")
                    new_changelog.append(f"No changes to {name}.")
                    new_changelog.append("")
                    has_added_entry = True

                new_changelog.append(line)
            with file.open("w") as f:
                f.write("\n".join(new_changelog))

    for file in version_files:
        file.write_text(file.read_text().replace(str(version), str(new_version), 1))
        if verbose:
            typer.echo(f"Bumped version from {version} to {new_version} in {file}.")

    typer.echo(f"Bumped version from {version} to {new_version} in {len(version_files)} files.")


# This is just for demo purposes, to test the secret plugin in the Toolkit CLI
import_app = typer.Typer(
    pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False
)


@import_app.command("cdf")
def cdf(
    ctx: typer.Context,
) -> None:
    """Import resources into Cognite Data Fusion."""
    print("Ran CDF Import Command")


CDF_TK_PLUGIN = {
    "bump": bump_app,
    "import": import_app,
}

if __name__ == "__main__":
    bump_app()
