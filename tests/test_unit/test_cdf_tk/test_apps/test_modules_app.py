import json
import os
import subprocess
import sys
from pathlib import Path

from cognite_toolkit._version import __version__


def test_modules_list_json_keeps_warnings_on_stderr(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("TOOLKIT_TEST_ENV=from_dotenv\n", encoding="utf-8")
    (tmp_path / "cdf.toml").write_text(
        f'[modules]\nversion = "{__version__}"\n\n[feature_flags]\nmodules-list-json = true\n',
        encoding="utf-8",
    )
    (tmp_path / "custom_modules").mkdir()
    raw_dir = tmp_path / "modules" / "my_module" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "example.Database.yaml").write_text("dbName: example\n", encoding="utf-8")

    env = os.environ | {"TOOLKIT_TEST_ENV": "from_environment", "SENTRY_ENABLED": "false", "COLUMNS": "200"}
    for key in ("CDF_PROJECT", "CDF_CLUSTER", "CDF_URL", "PYTHON_DOTENV_DISABLED"):
        env.pop(key, None)
    result = subprocess.run(
        [sys.executable, "-m", "cognite_toolkit._cdf", "modules", "list", "--format", "json"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert [module["module_name"] for module in payload["modules"]] == ["my_module"]
    assert "'feature_flags' section has been renamed to 'alpha_flags'" in result.stderr
    assert "Overriding the following environment variables" in result.stderr
    assert "TOOLKIT_TEST_ENV" in result.stderr
    assert "Directory custom_modules is deprecated" in result.stderr
