#!/usr/bin/env python
import os
import shutil
from pathlib import Path

THIS_FOLDER = Path(__file__).parent.absolute()
DEMO_PROJECT = THIS_FOLDER.parent / "demo_project"


def run() -> None:
    print("Running copy commands to prep deployment of demo...")
    os.makedirs(DEMO_PROJECT, exist_ok=True)
    print("Copying my demo.config.yaml to root of repo...")
    shutil.copy(THIS_FOLDER / "config.demo.yaml", DEMO_PROJECT / "config.demo.yaml")


if __name__ == "__main__":
    run()
