# local_modules directory

You are free to add your own modules to this directory as long as you don't use the `cdf_` prefix.
Each module should have a default.config.yaml file that contains variables that are used in the module. The
sub-directories in each module correspond to the different resources in CDF. See the [my_example_module](my_example_module/README.md)
for an example of a module. Run the command `cdf-tk init --upgrade` to add the variables from the default.config.yaml
into the `config.yaml` file in the root of your project directory. You can then override these default values in that
`config.yaml` file.

See the [module and package documentation](../docs/overview.md) for an introduction.
