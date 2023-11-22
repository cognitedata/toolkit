# Official Cognite Data Fusion Project configuration templates

> Configure Cognite Data Fusion quickly, consistently, traceably, and repeatably.
 
> [!WARNING]
> The templates and the `cdf-tk` tool are currently in **ALPHA**. The scope in alpha is on Asset
Performance Management focused on Infield (Digital Operator Rounds). The templates and tooling
will be continously improved throughout moving towards beta and general availability.


## Getting started

These are the steps to get started with Cognite Data Fusion configuration:

- Install the Cognite Data Fusion Toolkit in your repository.
- Use run the `init` command to get the modular resource packages to install in your Cognite Data Fusion projects
- Configure the included (and add your own) modules by editing the yaml configuration files to fit your needs
- Build, Verify and Deploy the configuration using the Toolkit
- Optional: set up automated deployment using GitHub Actions


## Prerequisites

- A working Python installation version >= 3.9 (3.11 is recommended)
- A target Cognite Data Fusion project[^1]
- Identity Provider groups and service principals, see [Developer Toolkit Identity Provider Configuration](https://developer.cognite.com/sdks/toolkit/idp).


## Step 1: Install the toolkit CLI 

To install the `cdf-tk` tool, you need a working Python installation >=3.9 (recommended 3.11). It is available as a command-line tool, and `cdf-tk --help` will give you available options:

```
pip install cognite-toolkit
cdf-tk --help
```

Follow instructions on [Developer Toolkit Quickstart](https://developer.cognite.com/sdks/toolkit/quickstart).


## Step 2: Configure modules

A **module** is a bundle of Cognite Data Fusion resources that are coupled together as logical units, for example data pipelines or the configuration of an application. For convenience, related modules are combined in _packages_. See [Developer Toolkit Templates](https://developer.cognite.com/sdks/toolkit/templates) for a more extensive description. 

To install the modules, run this command with <my_project_dir> of your own choosing:

```sh
cdf-tk init <my_project_dir> 
```


### Select which modules to deploy

Edit the `<my_project_dir>/local.yaml` file. You can add multiple environments (Cognite Data Fusion Projects[^1]) like this:

```yaml
# environment name
dev:
  project: <customer>-dev # the cdf project name
  type: dev
  deploy: # packages or modules to deploy to this environment
    - cdf_demo_infield 
    - cdf_apm_simple
```

The environment (dev in the example) is used by the cli tool as `--env <environment>` to determine which configurations to deploy.



### Configure each module as needed

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

Following the same pattern as above, custom modules can be added to `<my_project_dir>/local_modules/`. If you add these modules to your local.yaml, they will get included in the build. See advanced usage of templates in the [Developer Toolkit Advanced usage of templates](https://developer.cognite.com/sdks/toolkit/advanced).



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

Once satisfied with the output, the configuration can be applied in the target Cognite Data Fusion Project like this:

```
cdf-tk deploy --env <my_env> --build-dir <my_build_dir>
```

Note that there are option flags to delete previously created resources (deterministic) and data. Check `cdf-tk deploy --help` for information.




> [!NOTE]
> Make sure .env files are added to .gitognore so secrets aren't accidentally pushed to version control.
> It is also a good idea to add the `build_dir` to `.gitignore`to avoid checking it in to source control. `build_dir` defaults to `./build`.  


See [./CONTRIBUTING.md](./CONTRIBUTING.md) for information about how to contribute to the `cdf-tk` tool or
templates.

[^1]: A **Project** is the term for contained, stand-alone instance of Cognite Data Fusion that does not share configuration or data with other instances. It is identifyable by **project** in the API url by `https://{{cluster}}.cognitedata.com/api/v1/projects/{{project}}` and in the Fusion UI url `https://{{organisation}}.fusion.cognite.com/{{project}}`. A customer typically has at least a dev and a prod project.

