# Your CDF project

This directory contains the configuration files for your CDF project.

Check into Git all the files in this directory, but do not touch the modules
prefixed with `cdf_*`, only add config.yaml files to the root of each module
(alongside `default.config.yaml`) to override variables.  You can copy modules
and build your own modules in the `local_modules` directory. Remember to add the modules
you want to deploy to the environment you are using in `local.yaml`.

You can do `cdf-tk init --upgrade` in this directory to upgrade all the `cdf_*` modules to the
latest version. Your `config.yaml` files will not be touched.

Use this file to document your configuration files!

## Naming standards

A common structure on naming different CDF resource types and configuration are important from day one. Easy to use and understandable naming standard makes it easy to navigate and search on all components in a project as is grows in data sources, code, configuration, supported solutions and human resources working with and using the CDF solutions.

#### Separation tokens

* For external IDs the separation token is **’_’**  (underscore)  - this token works for all OS, when external ID matches usage of files
* For names the separation token is **’:’ or '_'**  (colon or underscore)  - these tokens matches usage in other CLI tools ang gives good readability

### Example usage of naming standard

In the example below we are setting up a project  based on the Open Industry Data (OID), that originates from the Valhall oil rig. Hnence the example location below is *valhall_oid*

* the **location_name** = valhall_oid
* The different data sources are:
  * workmate (asset & workorder data)
  * fileshare (files and 3D)
  * PI (time series / data points)

```
CDF project
│
├── Data Sets:
│   ├── extId: ds_asset_valhall_oid ── name: asset:valhall_oid
│   │   ├── Extraction Pipelines:
│   │   │   └── extId: ep_src_asset_valhall_oid ── name: src:asset:valhall_oid
│   │   │
│   │   ├── RAW DB/tables:
│   │   │   └── DB: src_asset_valhall_oid_workmate ── table: assets
│   │   │
│   │   ├── Transformations:
│   │   │   └── extId: tr_asset_valhall_oid_asset_hierarchy ── name: asset:valhall_oid:asset_hierarchy
│   │   │
│   │   └── Autorisation groups:
│   │       ├── id: asset:valhall_oid:extractor
│   │       ├── id: asset:valhall_oid:prosessing
│   │       └── id: asset:valhall_oid:read
│   │ 
│   ├── extId: ds_files_valhall_oid ── name: files:valhall_oid
│   │   ├── Extraction Pipelines:
│   │   │   ├── extId: ep_src_files_valhall_oid_fileshare ── name: src:files:valhall_oid:fileshare
│   │   │   └── extId: ep_ctx_files_valhall_oid_fileshare:annotation ── name: ctx:files:valhall_oid:fileshare:annotation
│   │   │
│   │   ├── RAW DB/tables:
│   │   │   └── DB: src_files_valhall_oid_fileshare ── table: file_metadata
│   │   │
│   │   ├── Transformations:
│   │   │   └── extId: tr_file_valhall_oid_fileshare_file_metadata ── name: file:valhall_oid:metadata:fileshare:file_metadata
│   │   │
│   │   ├── Functions:
│   │   │   └── extId: fu_files_valhall_oid_fileshare_annotation ── name: files:valhall_oid:fileshare:annotation
│   │   │
│   │   └── Autorisation groups:
│   │       ├── id: files:valhall_oid:extractor
│   │       ├── id: files:valhall_oid:prosessing
│   │       └── id: files:valhall_oid:read
│   │ 
│   ├── extId: ds_workorder_valhall_oid ── name: workorder:valhall_oid
│   │   ├── ...
│   │   ...
│   │
│   ├── extId: ds_timeseries_valhall_oid ── name: timeseries:valhall_oid
│   │   ├── ...
│   │   ... 
│   │
│   ├── extId: ds_3d_valhall_oid ── name: 3d:valhall_oid
│   │   ├── ...
│   │   ... 
│ 
└── Spaces:
    └── extId: sp_apm_valhall_oid ── name: valhall_oid
```


### Naming elements

* **Data Type:**  asset, timeseries, workorder, files, 3d,... (use what is relevant for project)
* **Source:**  Source system where data originates from (ex, SAP, Workmate, Aveva, PI, Fileshare, SharePoint,..)
* **Location:** Location for Asset / System / Plant / installation 
* **Pipeline Type:**  src = source data, ctx = contextualization, uc = use case, ...
* **Operation Type:** Type of operation/action/functionality in transformation or CDF function
* **Access Type:** Type of access used in authorization groups (ex: extractor, processing, read, ...)

#### Data sets:
```
External ID: ds_<data type>_<source>
Name: <data type>:<source>
Ex: ds_asset_valhall_oid / asset:valhall_oid 
```

#### Extraction Pipelines:
```
External ID: ep_<data type>_<source>_<location>
Name: <data type>:<source>:<location>
Ex: ds_asset_valhall_oid_workmate / asset:valhall_oid:workmate 
```

#### RAW DB/tables:
```
DB: <data type>_<source>_<location>
Ex: asset_valhall_oid_workmate
Table: use name from source, or other describing name 
```

#### Transformations:
```
External ID: tr_<data type>_<source>_<location>_<operation type>
Name: <data type>:<source>:<location>:<operation type>
Ex: tr_asset_valhall_oid_asset_hierarchy / asset:valhall_oid:asset_hierarchy 
```

#### Functions:
```
External ID: fu_<data type>_<source>_<location>_<operation type>
Name: <data type>:<source>:<location>:<operation type>
Ex: fu_files_valhall_oid_fileshare_annotation / files:valhall_oid:fileshare:annotation
```

#### Authorization groups:
```
Name: <data type>:<source>:<access type>
Ex:  asset:valhall:extractor / asset:valhall:processing / asset:valhall:read  
```

#### Data Model Spaces:
```
External ID: dm_<data type>_<source>
Name: <data type>:<source>
Ex: dm_apm_valhall_oid / apm:valhall_oid 
```



