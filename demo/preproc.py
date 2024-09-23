#!/usr/bin/env python
import argparse
import os
import shutil
from pathlib import Path

THIS_FOLDER = Path(__file__).parent.absolute()
DEMO_PROJECT = THIS_FOLDER.parent / "demo_project"

parser = argparse.ArgumentParser()
parser.add_argument("--modules", help="Which modules to run.", type=str, choices=["demo", "all"], default="demo")


def run() -> None:
    args = parser.parse_args()
    print("Running copy commands to prep deployment of demo...")
    os.makedirs(DEMO_PROJECT, exist_ok=True)
    print("Copying my demo.config.yaml to root of repo...")

    demo_config_path = THIS_FOLDER / "config.demo.yaml"
    destination_path = DEMO_PROJECT / "config.demo.yaml"
    # By default the 'selected_modules_and_packages' is the selected demo packages/modules
    if args.modules == "all":
        import yaml

        demo_config = yaml.safe_load(demo_config_path.read_text())
        demo_config["environment"]["selected"] = ["modules/"]
        destination_path.write_text(yaml.dump(demo_config))
        print(f"Updated {demo_config_path} to include all modules.")
    else:
        shutil.copy(demo_config_path, destination_path)
        print(f"Copied {demo_config_path} to {destination_path} with selected demo packages.")


if __name__ == "__main__":
    run()
