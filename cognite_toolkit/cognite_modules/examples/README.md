# EXAMPLE modules

This directory contains modules that are meant as examples or starting points for your own modules. You
should copy and rename an example module into the `custom_modules` directory (remove any `cdf_` prefixes) and make
your own modifications. You should then update the `deploy:` section in your `environments.yaml` file to install
the module.

Some of these modules also contain data that you can use to quickly get started without ingesting data
into CDF. The cdf_apm_simple module is a good example of this. It contains a small data set from [Open Industrial
Data](https://learn.cognite.com/open-industrial-data), the Valhall platform.
