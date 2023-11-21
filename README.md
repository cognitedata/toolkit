# Official Cognite Data Fusion project configuration templates

> Configure Cognite Data Fusion quickly, consistently, traceably, and repeatably
 
> [!NOTE]
> The templates and the `cdf-tk` tool are currently in **ALPHA**. The scope in alpha is on Asset
Performance Management focused on Infield (Digital Operator Rounds). The templates and tooling
will be continously improved throughout moving towards beta and general availability.


## Getting started
1. Install the Cognite Data Fusion Toolkit in your repository. It consists the `cdf-tk` tool and modular resource packages to install in your Cognite Data Fusion projects
1. Configure the included or custom modules by editing the yaml configuration files to fit your needs
1. Build, Verify and Deploy the configuration using `cdf-tk`
1. Optional: set up automated deployment using GitHub Actions

## 1. Install the toolkit CLI

To install the `cdf-tk` tool, you need a working Python installation >=3.9 (recommended 3.11). It is available as a command-line tool, and `cdf-tk --help` will give you available options:

```
pip install cognite-toolkit
cdf-tk --help
```

The Cognite Data Fusion Toolkit is a command-line interface that supports three different modes of operation:

1. As an **interactive command-line tool** used alongside the Cognite Data Fusion web application to retrieve and
   push configuration of the different Cognite Data Fusion services like data sets, data models, transformations,
   and more. This mode also supports configuration of new Cognite Data Fusion projects to quickly get started.
2. As tool to support the **project life-cyle by scripting and automating** configuration and management of Cognite Data
   Fusion projects where CDF configurations are kept as yaml-files that can be checked into version
   control. This mode also supports DevOps workflows with development, staging, and production projects.
3. As a **tool to deploy official Cognite project templates** to your Cognite Data Fusion project. The tool comes
   bundled with templates useful for getting started with Cognite Data Fusion, as well as for specific use cases
   delivered by Cognite or its partners. You can also create your own templates and share them.

More details about the tool can be found at
[developer.cognite.com](http://developer.cognite.com/sdks/toolkit).


## 2. Configure modules

A **module** is a bundle of Cognite Data Fusion resources that are coupled together as logical units, for example data pipelines or the configuration of an application.

To install the modules, run this command with <directory_name> of your own choosing:

```sh
cdf-tk init <directory_name> 
```

There are three kinds of modules: 

* `<directory_name>/modules/cdf_...`: Official Cognite modules. Do not make changes here, except adding your own config.yamls. These are potentially the only files that won't be overwritten in toolkit updates.
* `<directory_name>/local_modules/`: Customer-specific modules can be added here as you see fit
* `<directory_name>/examples/`: Sample configuration and data that can be useful for demonstrations or as boilerplate setup  

For convenience, modules are combined in _packages_. See [default.packages.yaml](/cognite_toolkit/default.packages.yaml) for examples.


### 2.1. Select which modules to deploy

Edit the `cognite_toolkit/local.yaml` file. You can add multiple environments (Cognite Data Fusion instances) like this:

```yaml
# environment name
dev:
  project: <customer>-dev # the cdf project name
  type: dev
  deploy: # packages or modules to deploy to this environment
    - cdf_demo_infield 
    - cdf_apm_simple
```

The environment (dev in the example) is used by the cli tool to determine which configurations to deploy.



### 2.1. Configure each module as needed

Each module follow a similar pattern:

```
modules/
   <module_name>/
      auth/
      data_models/
      transformations/
      raw/
      default.config.yaml 
      config.yaml
```
Create a copy of default.config.yaml to config.yaml and edit it according to the customer setup. In general, this is the only file that should be edited in the modules. The build command will replace placeholder values in the templates with these values.     

> [!WARNING]
>
> `default.config.yaml` files and the contents in the resource folders should not be edited as they might be overwritten by toolkit updates.
> The correct way to configure a module is to make a copy of `default.config.yaml` as `config.yaml` and make changes there. These files should be added to version control.


### 2.3 Create your own custom modules

Following the same pattern as above, custom modules can be added to `<directory_name>/local_modules/`. 



## 3. Build and verify the configuration

Run the `build` command with the `cdf-tk` CLI:

```sh
cdf-tk build --env <my_env> --build-dir <my_build_dir> 
```

This processes all configuration files in the selected packages and modules, and writes the output to <my_build_dir>. It also provides helpful hints and warnings. The build directory can be reviewed manually idf desired. Sending the `--clean` flag ensures that the build folder is empty so that previous builds don't interfere with the last run (deterministic).

> [!TIP]
> It is a good idea to add the `build_dir` to `.gitignore`to avoid checking it in to source control. `build_dir` defaults to `./build`.  



> Below is an overview of the scope of what can be governed through using these templates:

![Overview of project templates](./static/overview.png "Overview")

## Quickstart

## For more information



You can find an overview of the modules and packages in the
[module and package documentation](http://developer.cognite.com/sdks/toolkit/modules).

See [./CONTRIBUTING.md](./CONTRIBUTING.md) for information about how to contribute to the `cdf-tk` tool or
templates.
