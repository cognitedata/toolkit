# Main modules folder

** YOU SHOULD NOT EDIT ANY OF THE FILES IN THIS DIRECTORY AND SUB-DIRECTORIES **

Modules in this folder come bundled with the `cdf-tk` tool. They are managed
from a [public repository](https://github.com/cognitedata/cdf-project-templates).

The modules prefixed by `cdf_` are managed and supported by Cognite. You should put your own modules in
the local_modules/ directory.

In the root of this directory, you will find the `default.config.yaml` file that defines globally available
configuration variables. These can be used also in your own modules. For each of the modules, you will
find a `default.config.yaml` file that defines the default module-specific configuration variables.

As part of a `cdf-tk init`, these default variables will be copied to the `config.yaml` file in the
root of your project directory. You can then override these default values in that `config.yaml` file.

The modules are grouped into sub-directories:

* **common**: these modules are CDF project wide and are not specific to any particular solution.
* **examples**: these modules are meant to be copied to `local_modules`, renamed, and used as a starting point
  for your own modules.
* **<solution>**: e.g. apm and infield. These modules are specific to a particular solution. Typically,
  a solution like infield consists of multiple modules.

See the [module and package documentation](https://developer.cognite.com/sdks/toolkit/references/module_reference) for an introduction.
