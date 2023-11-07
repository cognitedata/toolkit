# Identity Provider (Authentication and Authorization)

You can use any Identity Provider like Azure Entra (aka Active Directory), Auth0, or others suppored by CDF.
The tools here will load information about the project and the identity provider from environment variables.

## CDF Environment

First, `CDF_CLUSTER` and `CDF_PROJECT` must be set to the CDF cluster and project you want to deploy to. These can
vary dependent on the environment you deploy to (as defined in [./local.yaml](./local.yaml) and specified in
`cdf.py build --env=<environment>`).

## Identity Provider

Second, to specify the information needed for the identity provider (and configured for the project), you only need to set
the IDP_TOKEN_URL. This URL typically ends in `/oauth/token` and for Entra and most hosted identity providers like Auth0,
the URL contains the tenant id or a part that identifies your tenant. An example is
`https://login.microsoftonline.com/your_tenant.onmicrosoft.com/oauth2/v2.0/token`.

## Service Account/Application

Third, the `cdf.py` tool (or the deployment pipeline used in your CI/CD setup) needs a service account/service principal
or application (they are called different things in different identity providers)
with access rights to allow it to write configurations. You create an application/service principal in your identity provider
with client credentials flow enabled (OAuth2). You will get a client id and a secret. These are the last two environment
variables needed:

```bash
IDP_CLIENT_ID=<client_id>
IDP_CLIENT_SECRET=<secret>
```

## Authorization Through CDF Groups

Fourth and final, you need to create a group in your identity provider that the application/
service principal is a member of. This group will be used to grant access to the CDF project
through the [./common/cdf_auth_readwrite_all](./common/cdf_auth_readwrite_all) module.
This module loads two useful groups that can be used with the templates. One group is
the read/write group for the `cdf.py` tool or your CI/CD pipeline. The other group is a read-only
group that can be used for admin users to log into the the Fusion UI and verify the configurations.

In the default recommended claims configuration, the identity provider's group memberships will be
included in the token that is issued to the application/service principal. The group id of the
group you have created for the application/service principal must thus be set in the
`readwrite_source_id` variable in the
[./common/cdf_auth_readwrite_all/default.config.yaml](./common/cdf_auth_readwrite_all/default.config.yaml) file.

> The .env.tmpl file can be used to set the necessary environment variables for local use of the scripts.
> When setting up a deployment pipeline, you should make sure that IDP_CLIENT_SECRET is not written
> in a file in the git repository, but set as an environment in the execution environment (Github
> Actions or similar). The source ids in the config file are not sensitive.
