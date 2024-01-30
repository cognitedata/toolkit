# Main Cognite Modules modules folder

Modules in this folder come bundled with the `cdf-tk` tool. They are managed
from a [public repository](https://github.com/cognitedata/cdf-project-templates).

The modules prefixed by `cdf_` are managed and supported by Cognite. You should put your own modules in
the custom_modules directory.

The modules are grouped into sub-directories:

* **common**: these modules are CDF project wide and are not specific to any particular solution.
* **examples**: these modules are meant to be copied to `custom_moudles`, renamed, and used as a starting point
  for your own modules.
* **<solution>**: e.g. apm and infield. These modules are specific to a particular solution. Typically,
  a solution like infield consists of multiple modules.

See the [module and package documentation](https://developer.cognite.com/sdks/toolkit/references/module_reference) for
an introduction.
