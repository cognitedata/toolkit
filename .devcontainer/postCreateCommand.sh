#!/usr/bin/env bash
set -e
cd /workspaces/toolkit
echo "Installing poetry packages into ./.venv..."
poetry config virtualenvs.in-project true && poetry env use python3
poetry --with=dev install --sync
echo "Installing pre-commit..."
poetry run pre-commit install
