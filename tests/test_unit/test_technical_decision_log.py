import re
from pathlib import Path

import pytest

from tests.constants import REPO_ROOT

TECHNICAL_DECISION_LOG_DIR = REPO_ROOT / "technical_decision_log"


def get_decision_files() -> list[Path]:
    """Get all TDL markdown files (excluding TEMPLATE.md and README.md)."""
    return [f for f in TECHNICAL_DECISION_LOG_DIR.glob("TDL-*.md") if f.name not in ("TEMPLATE.md", "README.md")]


@pytest.mark.parametrize("decision_file", get_decision_files(), ids=lambda p: p.name)
def test_decision_follows_format(decision_file: Path) -> None:
    content = decision_file.read_text(encoding="utf-8")

    assert re.match(r"^# TDL-\d{4}: .+", content), f"{decision_file.name} must start with '# TDL-XXXX: [title]'"
    assert "**Date:**" in content, f"{decision_file.name} must have a '**Date:**' field"
    assert "## Decision" in content, f"{decision_file.name} must have a '## Decision' section"
    assert "## Why" in content, f"{decision_file.name} must have a '## Why' section"
