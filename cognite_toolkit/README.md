# Your CDF project

This directory contains the configuration files for your CDF project.

Check into Git all the files in this directory, but do not touch the modules
prefixed with `cdf_*`, only add config.yaml files to the root of each module
(alongside `default.config.yaml`) to override variables.  You can copy modules
and build your own modules in the `local_modules` directory. Remember to add the modules
you want to deploy to the environment you are using in `local.yaml`.

You can do `cdf-tk init --upgrade` in this directory to upgrade all the `cdf_*` modules to the
latest version. Your `config.yaml` files will not be touched.

Use this file to document your configuration files!
