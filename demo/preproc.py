#!/usr/bin/env python
import os
import shutil
from pathlib import Path

THIS_FOLDER = Path(__file__).parent.absolute()
DEMO_PROJECT = THIS_FOLDER.parent / "demo_project"


def run() -> None:
    print("Running copy commands to prep deployment of demo...")
    os.makedirs(DEMO_PROJECT, exist_ok=True)
    print("Copying my enviroments.yaml to root of repo...")
    shutil.copy(THIS_FOLDER / "environments.yaml", DEMO_PROJECT / "environments.yaml")
    print("Copying config.yaml into demo project...")
    shutil.copy(THIS_FOLDER / "config.yaml", DEMO_PROJECT / "config.yaml")


if __name__ == "__main__":
    run()
