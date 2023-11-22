# Official Cognite Data Fusion Project configuration templates

> Configure Cognite Data Fusion quickly, consistently, traceably, and repeatably
 
> [!NOTE]
> The templates and the `cdf-tk` tool are currently in **ALPHA**. The scope in alpha is on Asset
Performance Management focused on Infield (Digital Operator Rounds). The templates and tooling
will be continously improved throughout moving towards beta and general availability.


## Getting started

This are the steps to get started with Cognite Data Fusion configuration:

- Install the Cognite Data Fusion Toolkit in your repository.
- Use run the `init` command to get the modular resource packages to install in your Cognite Data Fusion projects
- Configure the included (and add your own) by editing the yaml configuration files to fit your needs
- Build, Verify and Deploy the configuration using the Toolkit
- Optional: set up automated deployment using GitHub Actions

## Prerequisites

- A working Python installation
- A target Cognite Data Fusion project[^1]
- Identity Provider groups and service principals, see [https://developer.cognite.com/sdks/toolkit/idp](https://developer.cognite.com/sdks/toolkit/idp)


### Step 1: Install the toolkit CLI 

To install the `cdf-tk` tool, you need a working Python installation >=3.9 (recommended 3.11). It is available as a command-line tool, and `cdf-tk --help` will give you available options:

```
pip install cognite-toolkit
cdf-tk --help
```

> [!TIP]
> The Cognite Data Fusion Toolkit supports three different modes of operation:
>
> 1. As an **interactive command-line tool** used alongside the Cognite Data Fusion web application to retrieve and
>   push configuration of the different Cognite Data Fusion services like data sets, data models, transformations,
>   and more. This mode also supports configuration of new Cognite Data Fusion projects to quickly get started.
> 2. As tool to support the **project life-cyle by scripting and automating** configuration and management of Cognite Data
>   Fusion projects where CDF configurations are kept as yaml-files that can be checked into version
>   control. This mode also supports DevOps workflows with development, staging, and production projects.
> 3. As a **tool to deploy official Cognite project templates** to your Cognite Data Fusion project. The tool comes
>   bundled with templates useful for getting started with Cognite Data Fusion, as well as for specific use cases
>   delivered by Cognite or its partners. You can also create your own templates and share them.
>
> More details about the tool can be found at
> [developer.cognite.com](http://developer.cognite.com/sdks/toolkit).


### Step 2: Configure modules

A **module** is a bundle of Cognite Data Fusion resources that are coupled together as logical units, for example data pipelines or the configuration of an application. See https://developer.cognite.com/sdks/toolkit/templates for a more extensive description.

To install the modules, run this command with <my_project_dir> of your own choosing:

```sh
cdf-tk init <my_project_dir> 
```

There are three kinds of modules: 

* `<my_project_dir>/modules/cdf_...`: Official Cognite modules. Do not make changes here, except adding your own config.yamls. These are potentially the only files that won't be overwritten in toolkit updates.
* `<my_project_dir>/local_modules/`: Customer-specific modules can be added here as you see fit
* `<my_project_dir>/examples/`: Sample configuration and data that can be useful for demonstrations or as boilerplate setup  

For convenience, modules are combined in _packages_. See [default.packages.yaml](/cognite_toolkit/default.packages.yaml) for examples.


#### 2.1. Select which modules to deploy

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



#### 2.2. Configure each module as needed

Each module follow a similar pattern:

```
./modules|common|examples|local_modules/<moduleA>/
                                                |- data_models/
                                                |- auth/
                                                |- transformations/
                                                |- raw/
                                                |- time_series/
                                                |- files/
                                                default.config.yaml 
                                                config.yaml
```

Create a copy of default.config.yaml to config.yaml and edit it according to the customer setup. In general, this is the only file that should be edited in the modules. The build command will replace placeholder values in the templates with these values.     

> [!WARNING]
>
> `default.config.yaml` files and the contents in the resource folders should not be edited as they might be overwritten by toolkit updates.
> The correct way to configure a module is to make a copy of `default.config.yaml` as `config.yaml` and make changes there. These files should be added to version control.


#### 2.3 Create your own custom modules

Following the same pattern as above, custom modules can be added to `<my_project_dir>/local_modules/`. If you add these modules to your local.yaml, they will get included in the build. 



## Step 3: Build, verify and deploy the configuration

Run the `build` command with the `cdf-tk` CLI:

```sh
cdf-tk build --env <my_env> --build-dir <my_build_dir> 
```

This processes all configuration files in the selected packages and modules, and writes the output to <my_build_dir>. It also provides helpful hints and warnings. The build directory can be reviewed manually idf desired. Sending the `--clean` flag ensures that the build folder is empty so that previous builds don't interfere with the last run (deterministic). Repeat step 2 and 3 until you are satisfied with the output. 

Before deploying, you need to [set up and verify OIDC Connect Client Credentials][(https://developer.cognite.com/sdks/toolkit/quickstart](https://developer.cognite.com/sdks/toolkit/quickstart). You can either do this interactively or by using a .env file: 

- interactively: run `cdf-tk auth verify --interactive` 
- manually: copy the `.env.tmpl` file found in the <my_project_dir> to `.env` and change it to match your IdP details

Once build and authorisation has been verified, run deploy with the `--dry-run` flag set:

```
cdf-tk deploy --env <my_env> --build-dir <my_build_dir> --dry-run true
```



> [!TIP]
> Make sure .env files are added to .gitognore so secrets aren't accidantaly pushed to version control.
> It is also a good idea to add the `build_dir` to `.gitignore`to avoid checking it in to source control. `build_dir` defaults to `./build`.  



> Below is an overview of the scope of what can be governed through using these templates:

![Overview of project templates](./static/overview.png "Overview")

## Quickstart

## For more information



You can find an overview of the modules and packages in the
[module and package documentation](http://developer.cognite.com/sdks/toolkit/modules).

See [./CONTRIBUTING.md](./CONTRIBUTING.md) for information about how to contribute to the `cdf-tk` tool or
templates.

[^1]: A **Project** is the term for contained, stand-alone instance of Cognite Data Fusion that does not share configuration or data with other instances. It is identifyable by **project** in the API url by `https://{{cluster}}.cognitedata.com/api/v1/projects/{{project}}` and in the Fusion UI url `https://{{organisation}}.fusion.cognite.com/{{project}}`.

