# Migration Tests

This directory contains tests for comparing a project setup with the previous version of the packages
with the current version of the package.

## Motivation

This is to ensure that the migration scripts are working as expected. These tests are setup in a different directory
than the other tests as they are expensive to run due to the need to create one virtual environment for each version
of the package that we support migration from.

## Setup

To run the tests you need to setup virtual environments for each version of the package that you want to test.
This can be done by running the `create_environments.py` file in this directory.

```bash
python tests_migrations/create_environments.py
```

This will create a virtual environment. This is done as a separate step than running the tests as it is expensive,
and should thus be an explicit step.

Then, you can run the tests by running the `tests_migrations.py` file in this directory.

```bash
pytest tests_migrations/tests_migrations.py
```

 After running this file you folder structure should look like this:

```bash
tests_migrations
 ┣ 📂.venv_0.1.0b1 - Virtual environment for version 0.1.0b1
 ┣ 📂.venv_0.1.0b2
 ┣ 📂.venv_0.1.0b3
 ┣ 📂.venv_0.1.0b4
 ┣ 📂.venv_0.1.0b5
 ┣ 📂.venv_0.1.0b6
 ┣ 📂build - (created by running tests_migrations.py) Build directory for the modules
 ┣ 📂tmp-project - (created by running tests_migrations.py) Temporary project directory
 ┣ 📜constants - Contains which previous versions to tests against
 ┣ 📜create_environments.py - Creates virtual environments for each version to test against
 ┣ 📜tests_migrations.py - Runs the tests
 ┗ 📜README.md - This file.
```

## Tests

### <code>tests_init_migrate_build_deploy</code>

This tests runs the `init`, `build`, `deploy` command in previous versions of the package and
then runs the `build`, `deploy` in the current version. All it ensures is that the commands returns exit status 0.
There is no check that the deploy commands stays consistent between versions.
