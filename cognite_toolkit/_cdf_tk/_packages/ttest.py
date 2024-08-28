import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


with open(Path(__file__).parent / "_packages.toml", "rb") as f:
    packages = tomllib.load(f)["packages"]
    for k, v in packages["items"].items():
        print(k)
