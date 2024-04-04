from pathlib import Path

PYTEST_PROJECT = Path(__file__).parent / "project_for_test"
BUILD_DIR = PYTEST_PROJECT.parent / "tmp"
CUSTOM_PROJECT = Path(__file__).parent / "project_no_cognite_modules"
PROJECT_WITH_DUPLICATES = Path(__file__).parent / "project_with_duplicates"
