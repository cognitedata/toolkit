# Cognite Data Fusion Toolkit

[![release](https://img.shields.io/github/actions/workflow/status/cognitedata/toolkit/release.yaml?style=for-the-badge)](https://github.com/cognitedata/toolkit/actions/workflows/release.yaml)
[![Github](https://shields.io/badge/github-cognite/toolkit-green?logo=github&style=for-the-badge)](https://github.com/cognitedata/toolkit)
[![PyPI](https://img.shields.io/pypi/v/cognite-toolkit?style=for-the-badge)](https://pypi.org/project/cognite-toolkit/)
[![Downloads](https://img.shields.io/pypi/dm/cognite-toolkit?style=for-the-badge)](https://pypistats.org/packages/cognite-toolkit)
[![GitHub](https://img.shields.io/github/license/cognitedata/toolkit?style=for-the-badge)](https://github.com/cognitedata/toolkit/blob/master/LICENSE)
[![Docker Pulls](https://img.shields.io/docker/pulls/cognite/toolkit?style=for-the-badge)](https://hub.docker.com/r/cognite/toolkit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=for-the-badge)](https://github.com/astral-sh/ruff)
[![mypy](https://img.shields.io/badge/mypy-checked-000000.svg?style=for-the-badge&color=blue)](http://mypy-lang.org)

The CDF Toolkit is a command-line interface (`cdf`) used for configuring and administrating
Cognite Data Fusion (CDF) projects. It ships with modularised `templates` that helps you
configure Cognite Data Fusion according to best practices.

It supports three different modes of operation:

1. As an **interactive command-line tool** used alongside the Cognite Data Fusion web application to retrieve and
   push configuration of the different Cognite Data Fusion services like data sets, data models, transformations,
   and more. This mode also supports configuration of new Cognite Data Fusion projects to quickly get started.
2. As tool to support the **project life-cyle by scripting and automating** configuration and management of Cognite Data
   Fusion projects where CDF configurations are kept as yaml-files that can be checked into version
   control. This mode also supports DevOps workflows with development, staging, and production projects.
3. As a **tool to deploy official Cognite project templates** to your Cognite Data Fusion project. The tool comes
   bundled with templates useful for getting started with Cognite Data Fusion, as well as for specific use cases
   delivered by Cognite or its partners. You can also create your own templates and share them.

## Usage

Install the Toolkit by running:

```bash
pip install cognite-toolkit
```

Then run `cdf --help` to get started with the interactive command-line tool.

## For more information

More details about the tool can be found at
[docs.cognite.com](https://docs.cognite.com/cdf/deploy/cdf_toolkit/).

You can find an overview of the modules and packages in the
[module and package documentation](https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/resource_library).

See [./CONTRIBUTING.md](./CONTRIBUTING.md) for information about how to contribute to the `cdf-tk` tool or
templates.
