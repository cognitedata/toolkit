# Fabric OneLake external data sources

Deploy Fabric OneLake credentials and lakehouse location as toolkit resources under `transformations/`.

## Files

- `fabric_lakehouse_prod.ExternalDataSource.yaml` — registers the OneLake source in CDF
- `onelake_to_assets.Transformation.yaml` + `onelake_to_assets.sql` — example transform reading via `ext_onelake()`

## Prerequisites

- CDF project with Fabric enabled
- `transformationsExternalDataSourcesAcl` READ/WRITE on the deploying principal
- Environment variables at deploy time: `FABRIC_CLIENT_ID`, `FABRIC_TENANT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_WORKSPACE`, `FABRIC_LAKEHOUSE`

## Deploy order

Toolkit resolves `ext_onelake('fabric-lakehouse-prod', ...)` references in transformation SQL and deploys the external data source before the transformation.
