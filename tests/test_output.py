import subprocess

import pytest

@pytest.mark.skip(reason="Not implemented yet")
def test_print_clean_warnings() -> None:
    clean_cmd = ["cdf-tk", "deploy", "--env", "dev", "--dry-run"]
    output = subprocess.run(clean_cmd, capture_output=True, shell=True)

    message = output.stderr.decode() or output.stdout.decode()
    assert output.returncode == 0, f"Failed to run {clean_cmd[0]}: {message}"
