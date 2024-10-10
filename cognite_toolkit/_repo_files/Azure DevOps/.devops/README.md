# Using Toolkit to verify and deploy modules in Azure DevOps Pipelines

This guide will help you use the Cognite Toolkit in Azure DevOps
Pipelines. It assumes you have run the `cdf repo init` command,
selected Azure DevOps as the CI/CD provider, and pushed the
contents of `.devops/` to the main branch of your DevOps repository.

## Validate pull requests

Running an automatic check on pull requests is a good way to ensure
that the module is valid before merging it into the main branch.

Follow these steps:

### Step 1: Create a dry-run pipeline

1. In your [DevOps project](https://dev.azure.com), go to "Pipelines"
1. Click "New pipeline"
1. Select the repository
1. Select Existing Azure Pipelines YAML file
   - Branch: main
   - Path: `./devops/dry-run-pipeline.yml`
1. Click Save (alternative to Run button)
1. In the overview, click the three dots and select "Rename/move" to set a descriptive name, for example "Pull request checks".

### Step 2: Create variable groups for the dev environment

1. In your DevOps project, go to **Pipelines → Library**
1. Click **+ Variable group**
1. Create a variable group with the name `dev-toolkit-credentials`
1. Enter these variables with the correct values:
   - CDF_CLUSTER
   - CDF_PROJECT
   - LOGIN_FLOW: client_credentials
   - IDP_CLIENT_ID
   - IDP_CLIENT_SECRET (**important**: mark as secret with the padlock symbol)
   - IDP_TOKEN_URL (if not using Entra ID)
1. Click Pipeline permissions and select the pipelines that should have access to this variable group

### Step 3: Add the check to the branch policy

1. In your DevOps project, go to Repos → Branches
2. Click the three dots next to the main branch and select Branch policies
3. Click the `+` on Build Validation
4. Select the pipeline you created in step 1

Make sure you have enabled a minimum numbers of reviewers (1) too.

From now on, all new Pull requests will require a build and dry-run to succeed before merging.

## Deploy modules

To deploy modules automatically, you can create a pipeline that
builds and deploys the modules when changes are pushed to a
given branch, typically `main`.

### Step 1: Create a deployment pipeline

1. In your [DevOps project](https://dev.azure.com), go to "Pipelines"
2. Click "New pipeline"
3. Select the repository
4. Select Existing Azure Pipelines YAML file
   - Branch: main
   - Path: `./devops/deploy-pipeline.yml`
5. Click Save (alternative to Run button)

### Step 2: Create variable groups for the dev environment

If this variable group exists from the previous step, just add the new pipeline to the Pipeline permissions.

1. In your DevOps project, go to **Pipelines → Library**
1. Click **+ Variable group**
1. Create a variable group with the name `dev-toolkit-credentials`
1. Enter these variables with the correct values:
   - CDF_CLUSTER
   - CDF_PROJECT
   - LOGIN_FLOW: client_credentials
   - IDP_CLIENT_ID
   - IDP_CLIENT_SECRET (**important**: mark as secret with the padlock symbol)
   - IDP_TOKEN_URL (if not using Entra ID)
1. Click Pipeline permissions and select the pipelines that should have access to this variable group

The pipeline will now deploy the modules automatically when changes are pushed to the main branch.
