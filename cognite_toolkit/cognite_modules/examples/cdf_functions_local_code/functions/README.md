# How to deploy functions

Add function functions by creating a folder for each function and putting a `function_config.yaml`
file into that. If you want to schedule the function add a `schedule.yaml` file too.

By default the function code is exepcted to be in the function folder. But it's possible to override
it by changing the `folder` field in the config file. It can also be a templated field that is set dynamically in CI.

See the [example config file](./repeater/function_config.yaml).
