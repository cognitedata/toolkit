#!/usr/bin/env python

# You use this file to run the cdf-tk file for development purposes in Visual Studio Code
# to avoid installing the cognite-toolkit package.
# cdf.py is found inside the cognite-toolkit package, which is fine when you do pip install cognite-toolkit
# However, when you run the file in Visual Studio Code, the module should not be installed in your
# python virtual environment, but rather be found in the root of the repo.
# This workaround allows you to run cdf.py in Visual Studio Code like this:
"""{
    "name": "Python: build",
    "type": "python",
    "request": "launch",
    "program": "./cdf-tk-dev.py",
    "args": [
        "--verbose",
        "--override-env",
        "build",
        "--build-dir=build",
        "--clean",
        "--env=local",
        "./cognite_toolkit/"
    ],
    "console": "integratedTerminal",
    "justMyCode": false
},
"""

import os
import sys
from pathlib import Path

from cognite_toolkit._cdf_tk.data_classes import Environment, InitConfigYAML

REPO_ROOT = Path(__file__).resolve().parent
root_folder = rf"{REPO_ROOT.absolute()}"

sys.path.append(root_folder)
# Avoid sending requests to sentry when doing development
os.environ["SENTRY_ENABLED"] = "false"

from cognite_toolkit._cdf import app  # noqa: E402

if not Path(REPO_ROOT / "cognite_toolkit" / "config.local.yaml").exists():
    config_init = InitConfigYAML(
        Environment(
            name="local",
            project="<my-project>",
            validation_type="dev",
            selected=["cdf_demo_infield", "cdf_oid_example_data"],
        )
    ).load_defaults(REPO_ROOT / "cognite_toolkit")
    (REPO_ROOT / "cognite_toolkit" / "config.local.yaml").write_text(config_init.dump_yaml_with_comments())

if __name__ == "__main__":
    app()
