"""This is a small CLI used to develop Toolkit.

It is not used in production and is only intended to be used for development purposes.
"""

import itertools
import re
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, get_args

import marko
import marko.block
import marko.element
import marko.inline
import typer
from packaging.version import Version, parse
from rich import print

# Imported first, as importing the rules or data classes packages first fails on a circular import.
import cognite_toolkit._cdf_tk.commands  # noqa: F401
from cognite_toolkit._cdf_tk.data_classes._insights import RuleDefinition
from cognite_toolkit._cdf_tk.rules import get_rules_library

REPO_ROOT = Path(__file__).parent
CHANGELOG = REPO_ROOT / "CHANGELOG.cdf-tk.md"
IMAGE_NAME = "cognite/toolkit"
CDF_TOML = REPO_ROOT / "cdf.toml"

VALID_CHANGELOG_HEADERS = {"Added", "Changed", "Removed", "Fixed", "Improved"}
BUMP_OPTIONS = Literal["major", "minor", "patch", "skip"]
VALID_BUMP_OPTIONS = get_args(BUMP_OPTIONS)
LAST_GIT_MESSAGE_FILE = REPO_ROOT / "last_git_message.txt"
CHANGELOG_ENTRY_FILE = REPO_ROOT / "last_changelog_entry.md"
LAST_VERSION = REPO_ROOT / "last_version.txt"
RULES_LIBRARY_MDX = Path("docs/cdf/deploy/cdf_toolkit/references/rules_library.mdx")
RULES_TABLE_JSX = Path("docs/snippets/toolkit/rules-table.jsx")
# Resource types kept capitalized when listed after the first resource type in the rules library tables.
PROPER_NOUN_PREFIXES = ("CDF", "InField", "RAW", "Streamlit")
CODE_HEADING = re.compile(r"^### ([A-Z-]+) \{#[a-z-]+\}$")
JSX_RULES_ARRAY = re.compile(r"(const TOOLKIT_RULES = \[\n).*?(\n\];)", re.DOTALL)
VERSION_PLACEHOLDER = "0.0.0"
VERSION_FILES = [
    REPO_ROOT / "pyproject.toml",
    REPO_ROOT / "cognite_toolkit" / "_version.py",
    REPO_ROOT / "cdf.toml",
    *(REPO_ROOT / "tests" / "data").rglob("cdf.toml"),
    *(REPO_ROOT / "tests" / "data").rglob("_build_environment.yaml"),
    *(REPO_ROOT / "cognite_toolkit").rglob("cdf.toml"),
    REPO_ROOT / "cognite_toolkit" / "_repo_files" / "GitHub" / ".github" / "workflows" / "dry-run.yaml",
    REPO_ROOT / "cognite_toolkit" / "_repo_files" / "GitHub" / ".github" / "workflows" / "deploy.yaml",
]

DOCKER_IMAGE_FILES = [
    *(REPO_ROOT / "cognite_toolkit" / "_repo_files").rglob("*.yml"),
    *(REPO_ROOT / "cognite_toolkit" / "_repo_files").rglob("*.yaml"),
]

app = typer.Typer(
    add_completion=False,
    help=__doc__,
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


@app.command()
def bump(verbose: bool = False) -> None:
    last_version_str = LAST_VERSION.read_text().strip().removeprefix("v")
    try:
        last_version = parse(last_version_str)
    except ValueError:
        print(f"Invalid last version: {last_version_str}")
        raise SystemExit(1)

    changelog_items, _ = _read_last_commit_message()
    version_bump = _get_change(changelog_items)
    if version_bump == "skip":
        print("No changes to release.")
        return
    if version_bump == "major":
        new_version = Version(f"{last_version.major + 1}.0.0")
    elif version_bump == "minor":
        new_version = Version(f"{last_version.major}.{last_version.minor + 1}.0")
    elif version_bump == "patch":
        new_version = Version(f"{last_version.major}.{last_version.minor}.{last_version.micro + 1}")
    else:
        raise typer.BadParameter("You must specify one of major, minor, patch, alpha, or beta.")

    for file in VERSION_FILES:
        file.write_text(file.read_text().replace(str(VERSION_PLACEHOLDER), str(new_version), 1))
        if verbose:
            typer.echo(f"Bumped version from {last_version} to {new_version} in {file}.")
    for file in DOCKER_IMAGE_FILES:
        file.write_text(file.read_text().replace(f"{IMAGE_NAME}:{last_version!s}", f"{IMAGE_NAME}:{new_version!s}", 1))
        if verbose:
            typer.echo(f"Bumped version from {last_version} to {new_version} in {file}.")

    typer.echo(f"Bumped version from {last_version} to {new_version} in {len(VERSION_FILES)} files.")


@app.command("alpha")
def set_alpha(off: bool = False) -> None:
    if not off:
        return
    is_feature_flag = False
    new_lines = []
    for line in CDF_TOML.read_text().splitlines():
        if header_match := re.match(r"\[(\w+)\]", line.strip()):
            header = header_match.group(1)
            print(header)
            if header == "alpha_flags":
                is_feature_flag = True
            else:
                is_feature_flag = False
        if is_feature_flag:
            line = line.replace("true", "false")
        new_lines.append(line)

    CDF_TOML.write_text("\n".join(new_lines) + "\n")


@app.command("changelog")
def create_changelog_entry() -> None:
    changelog_items, changelog_text = _read_last_commit_message()
    version_bump = _get_change(changelog_items)
    if version_bump == "skip":
        print("No changes to release.")
        return
    if not changelog_items[1:]:
        print(f"Trying to {version_bump} bump but no changes found in the changelog.")
        raise SystemExit(1)
    if not _is_header(changelog_items[1], level=2, text="Changelog"):
        print("The first header in the changelog must be '## Changelog'.")
        raise SystemExit(1)
    change_entries = list(itertools.takewhile(lambda x: not _is_header(x, level=2), changelog_items[2:]))
    _validate_entries(change_entries, "Changelog")

    changelog_entry = changelog_text.split("## Changelog")[1]
    CHANGELOG_ENTRY_FILE.write_text(f"## Changelog {changelog_entry}", encoding="utf-8")
    print(f"Changelog entry written to {CHANGELOG_ENTRY_FILE}.")


def _validate_no_test_plan(pr_description: str) -> None:
    if "This PR was generated by [Mend Renovate]" in pr_description:
        return
    for item in marko.parse(pr_description).children:
        if not isinstance(item, marko.block.Heading) or item.level != 2:
            continue
        if not isinstance(item.children[0], marko.inline.RawText):
            continue
        if item.children[0].children.strip().casefold() == "test plan":
            print("PR description must not contain a '## Test plan' section.")
            raise SystemExit(1)


def _read_last_commit_message() -> tuple[list[marko.element.Element], str]:
    last_git_message = LAST_GIT_MESSAGE_FILE.read_text()
    _validate_no_test_plan(last_git_message)
    if "This PR was generated by [Mend Renovate]" in last_git_message:
        print("Skipping Renovate PR.")
        changelog_text = "- [ ] Patch\n- [ ] Minor\n- [x] Skip\n"
    elif "## Bump" not in last_git_message:
        print("No bump entry found in the last commit message.")
        raise SystemExit(1)
    else:
        changelog_text = last_git_message.split("## Bump")[1].strip()

    if "-----" in changelog_text:
        # Co-authors section
        changelog_text = changelog_text.split("-----")[0].strip()
    # Remove all lines that starts with "Co-authored-by:" as these are not part of the changelog
    # Note the co-author section is not always separated by "-----"
    changelog_text = "\n".join(
        line for line in changelog_text.splitlines() if not line.strip().startswith("Co-authored-by")
    ).strip()

    changelog_items = [
        item for item in marko.parse(changelog_text).children if not isinstance(item, marko.block.BlankLine)
    ]
    if not changelog_items:
        print("No changelog items found in the last commit message.")
        raise SystemExit(1)

    return changelog_items, changelog_text


def _is_header(item: marko.element.Element, level: int, text: str | None = None):
    if not (
        isinstance(item, marko.block.Heading)
        and item.level == level
        and isinstance(item.children[0], marko.inline.RawText)
    ):
        return False
    return text is None or item.children[0].children == text


def _get_change(changelog_items: list[marko.element.Element]) -> Literal["major", "minor", "patch", "skip"]:
    item = changelog_items[0]
    if not isinstance(item, marko.block.List):
        print("The first item in the changelog must be a list with the type of change.")
        raise SystemExit(1)
    selected: list[Literal["major", "minor", "patch", "skip"]] = []
    for child in item.children:
        if not isinstance(child, marko.block.ListItem):
            print(f"Unexpected item in changelog: {child}")
            raise SystemExit(1)
        if not isinstance(child.children[0], marko.block.Paragraph):
            print(f"Unexpected item in changelog: {child.children[0]}")
            raise SystemExit(1)
        if not isinstance(child.children[0].children[0], marko.inline.RawText):
            print(f"Unexpected item in changelog: {child.children[0].children[0]}")
            raise SystemExit(1)
        list_text = child.children[0].children[0].children
        if list_text.startswith("[ ]"):
            continue
        elif list_text.lower().startswith("[x]"):
            change_type = list_text.removeprefix("[x]").removeprefix("[X]").strip()
            if change_type.casefold() not in VALID_BUMP_OPTIONS:
                print(f"Unexpected change type in changelog: {change_type}")
                raise SystemExit(1)
            selected.append(change_type.casefold())
        else:
            print(f"Unexpected item in changelog: {list_text}")
            raise SystemExit(1)

    if len(selected) > 1:
        print("You can only select one type of change.")
        raise SystemExit(1)
    if not selected:
        print("You must select a type of change.")
        raise SystemExit(1)
    return selected[0]


def _validate_entries(items: list[marko.element.Element], section: str) -> None:
    seen_headers: set[str] = set()
    if not items:
        print(f"No entries found in the {section} section of the changelog.")
        raise SystemExit(1)
    if (
        isinstance(items[0], marko.block.Paragraph)
        and isinstance(items[0].children[0], marko.inline.RawText)
        and "no changes" in items[0].children[0].children.casefold()
    ):
        return
    last_header: str = ""
    for item in items:
        if isinstance(item, marko.block.Heading):
            if last_header:
                print(f"Expected a list of changes after the {last_header} header.")
                raise SystemExit(1)
            elif item.level != 3:
                print(f"Unexpected header level in changelog: {item}. Should be level 3.")
                raise SystemExit(1)
            elif not isinstance(item.children[0], marko.inline.RawText):
                print(f"Unexpected header in changelog: {item}.")
                raise SystemExit(1)
            header_text = item.children[0].children
            if header_text not in VALID_CHANGELOG_HEADERS:
                print(f"Unexpected header in changelog: {VALID_CHANGELOG_HEADERS}.")
                raise SystemExit(1)
            if header_text in seen_headers:
                print(f"Duplicate header in changelog: {header_text}.")
                raise SystemExit(1)
            seen_headers.add(header_text)
            last_header = header_text
        elif isinstance(item, marko.block.List):
            if not last_header:
                print("Expected a header before the list of changes.")
                raise SystemExit(1)
            last_header = ""
        else:
            print(f"Unexpected item in changelog: {item}.")
            raise SystemExit(1)


@app.command("rules-docs")
def generate_rules_docs(
    docs_repo: Path = typer.Argument(..., help="Path to a clone of the docs.cognite.com repository."),
) -> None:
    """Regenerates the rule tables of the Toolkit rules library page in the docs repository.

    The code sections, and their descriptions and fixes, are written by hand. Only the table in each code section
    and the rows of the filterable table are generated.
    """
    rules = get_rules_library()
    mdx_path, jsx_path = docs_repo / RULES_LIBRARY_MDX, docs_repo / RULES_TABLE_JSX
    mdx, missing_sections, unknown_sections = replace_mdx_tables(mdx_path.read_text(encoding="utf-8"), rules)
    mdx_path.write_text(mdx, encoding="utf-8", newline="\n")
    jsx_path.write_text(replace_jsx_rows(jsx_path.read_text(encoding="utf-8"), rules), encoding="utf-8", newline="\n")
    print(f"Wrote {len(rules)} rules to {mdx_path} and {jsx_path}.")
    for code in missing_sections:
        print(f"[yellow]Add a section '### {code} {{#{code.lower()}}}' with a table to {mdx_path}.[/yellow]")
    for code in unknown_sections:
        print(f"[yellow]The section for {code} has no rules. Remove it or move it to the reserved codes.[/yellow]")


def replace_mdx_tables(content: str, rules: Sequence[RuleDefinition]) -> tuple[str, list[str], list[str]]:
    """Replaces the first table after each '### CODE' heading with the rules of the code.

    Returns the new content, the codes without a section, and the sections without rules.
    """
    rules_by_code: dict[str, list[RuleDefinition]] = defaultdict(list)
    for rule in rules:
        rules_by_code[rule.code].append(rule)
    output: list[str] = []
    seen_codes: list[str] = []
    code: str | None = None
    in_table = False
    for line in content.split("\n"):
        if heading := CODE_HEADING.match(line):
            code, in_table = heading.group(1), False
            seen_codes.append(code)
        elif line.startswith("|") and code is not None:
            if not in_table and code in rules_by_code:
                output.append(render_mdx_table(rules_by_code[code]))
            in_table = True
            if code in rules_by_code:
                continue
        elif in_table:
            in_table, code = False, None
        output.append(line)
    missing = [code for code in rules_by_code if code not in seen_codes]
    unknown = [code for code in seen_codes if code not in rules_by_code]
    return "\n".join(output), missing, unknown


def render_mdx_table(rules: Sequence[RuleDefinition]) -> str:
    """A Markdown table of the rules of one code. The 'Reported when' column is only needed for several rules."""
    header = ["Resource type", "Severity", "Context", "Alpha"]
    show_reported_when = len(rules) > 1
    if show_reported_when:
        header.append("Reported when")
    rows = [
        [
            _display_resource_types(rule.resource_types),
            rule.severity,
            rule.context,
            "Yes" if rule.alpha else "No",
            *([rule.reported_when] if show_reported_when else []),
        ]
        for rule in rules
    ]
    widths = [max(len(cell) for cell in column) for column in zip(header, *rows)]

    def as_line(cells: list[str]) -> str:
        return "| " + " | ".join(cell.ljust(width) for cell, width in zip(cells, widths)) + " |"

    separator = "|" + "|".join("-" * (width + 2) for width in widths) + "|"
    return "\n".join([as_line(header), separator, *(as_line(row) for row in rows)])


def _display_resource_types(resource_types: Sequence[str]) -> str:
    """For example, 'View, container', following sentence case."""
    return ", ".join(
        name if index == 0 or name.startswith(PROPER_NOUN_PREFIXES) else name[:1].lower() + name[1:]
        for index, name in enumerate(resource_types)
    )


def replace_jsx_rows(content: str, rules: Sequence[RuleDefinition]) -> str:
    """Replaces the rows of the TOOLKIT_RULES array in the filterable rules table."""
    if not JSX_RULES_ARRAY.search(content):
        raise ValueError("Could not find 'const TOOLKIT_RULES = [' in the rules table snippet.")
    rows = "\n".join(_jsx_row(rule) for rule in rules)
    return JSX_RULES_ARRAY.sub(lambda match: f"{match.group(1)}{rows}{match.group(2)}", content, count=1)


def _jsx_row(rule: RuleDefinition) -> str:
    resources = ", ".join(_js_string(name) for name in rule.resource_types)
    # The table cells are plain text, so Markdown code spans would show their backticks.
    summary = _js_string(rule.reported_when.replace("`", ""))
    return (
        f"  {{ code: {_js_string(rule.code)}, resources: [{resources}], severity: {_js_string(rule.severity)}, "
        f"context: {_js_string(rule.context)}, alpha: {str(rule.alpha).lower()}, summary: {summary} }},"
    )


def _js_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


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
    "bump": app,
    "import": import_app,
}

if __name__ == "__main__":
    app()
