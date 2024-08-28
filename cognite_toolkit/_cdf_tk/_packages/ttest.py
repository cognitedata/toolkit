import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


with open(Path(__file__).parent / "packages.toml", "rb") as f:
    t = tomllib.load(f)
    for k, v in t["included"]["packages"]["items"].items():
        print(f"{k}: {v}")
