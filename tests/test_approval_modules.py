"""
Approval test takes a snapshot of the results and then compare them to last run, ref https://approvaltests.com/,
and fails if they have changed.

If the changes are desired, you can update the snapshot by running `pytest --force-regen`.
"""
from collections.abc import Iterator
from pathlib import Path

import pytest

from cdf_project_template.templates import TMPL_DIRS

REPO_ROOT = Path(__file__).parent.parent


def find_all_modules() -> Iterator[Path]:
    for tmpl_dir in TMPL_DIRS:
        for module in (REPO_ROOT / tmpl_dir).iterdir():
            if module.is_dir():
                yield pytest.param(module, id=f"{module.parent.name}/{module.name}")


@pytest.mark.parametrize("module_path", list(find_all_modules()))
def test_module_approval(module_path: Path, tmp_path: Path) -> None:
    print("Running module approval test for", module_path)
