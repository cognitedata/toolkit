# Cognite Data Model-Based Annotation Function

## Overview

The Annotation template is a framework designed to automate the process of annotating files within Cognite Data Fusion (CDF). It uses a data model-centric approach to manage the annotation lifecycle, from selecting files to processing results and generating reports. This template is configurable, allowing it to adapt to various data models and annotation requirements.

## Key Features

- **Configuration-Driven Workflow:** The entire process is controlled by a single `config.yaml` file, allowing adaptation to different data models and operational parameters without code changes.
- **Large Document Support (\>50 Pages):** Automatically handles files with more than 50 pages by breaking them into manageable chunks, processing them iteratively, and tracking the overall progress.
- **Parallel Execution Ready:** Designed for concurrent execution with a robust optimistic locking mechanism to prevent race conditions when multiple finalize function instances run in parallel.
- **Detailed Reporting:** Local logs and processed annotation details stored in CDF RAW tables, fucntion logs, and extraction pipeline runs for auditing and analysis.
- **Local Running and Debugging:** Both the launch and finalize handler can be ran locally and have default setups in the 'Run & Debug' tab in vscode. Requires a .env file to be placed in the directory.

## Getting Started

Deploying this annotation module into a new Cognite Data Fusion (CDF) project is a streamlined process. Since all necessary resources (Data Sets, Extraction Pipelines, Functions, etc.) are bundled into a single module, you only need to configure one file to get started.

### Prerequisites

- Python 3.11+
- An active Cognite Data Fusion (CDF) project.
- The required Python packages are listed in the `cdf_file_annotation/functions/fn_file_annotation_launch/requirements.txt` and `cdf_file_annotation/functions/fn_file_annotation_finalize/requirements.txt` files.
- Alias and tag generation is abstracted out of the annotation function. Thus, you'll need to create a transformation that populates the `aliases` and `tags` property of your file and target entity view.
  - The `aliases` property is used to match files with entities and should contain a list of alternative names or identifiers that can be found in the files image.
  - The `tags` property serves multiple purposes and consists of the following...
    - (`DetectInDiagrams`) Identifies files and assets to include as entities filtered by primary scope and secondary scope (if provided).
    - (`ScopeWideDetect`) Identifies files and asset to include as entities filtered by a primary scope.
    - (`ToAnnotate`) Identifies files that need to be annotated.
    - (`AnnotationInProcess`) Identifies files that are in the process of being annotated.
    - (`Annotated`) Identifies files that have been annotated.
    - (`AnnotationFailed`) Identifies files that have failed the annotation process. Either by erroring out or by receiving 0 possible matches.
  - Don't worry if these concepts don't immediately make sense. Aliases and tags are explained in greater detail in the detailed_guides/ documentation. The template also includes a jupyter notebook that prepare the files and assets for annotation if using the toolkit's quickstart module.

### Deployment Steps

_(if videos fail to load, try loading page in incognito or re-sign into github)_

1. **Create a CDF Project through Toolkit**
   - Follow the guide [here](https://docs.cognite.com/cdf/deploy/cdf_toolkit/)
   - (optional) Initialize the quickstart package using toolkit CLI
 ```bash
 poetry init
 poetry add cognite-toolkit
 poetry run cdf modules init <project-name>
 ```

<video src="https://github.com/user-attachments/assets/4dfa8966-a419-47b9-8ee1-4fea331705fd"></video>

<video src="https://github.com/user-attachments/assets/bc165848-5f8c-4eff-9a38-5b2288ec7e23"></video>

2. **Integrate the Module**
   - Move the `local_setup/` folder to the root and unpack .vscode/ and .env.tmpl
   - Update the default.config.yaml file with project-specific configurations
   - Add the module name to the list of selected modules in your config.{env}.yaml file
   - Make sure to create a .env file with credentials pointing to your CDF project

<video src="https://github.com/user-attachments/assets/78ef2f59-4189-4059-90d6-c480acb3083e"></video>

<video src="https://github.com/user-attachments/assets/32df7e8b-cc27-4675-a813-1a72406704d5"></video>

3. **Build and Deploy the Module**

   - (optional) Build and deploy the quickstart template modules
   - Build and deploy this module

 ```bash
 poetry run cdf build --env dev
 poetry run cdf deploy --dry-run
 poetry run cdf deploy
 ```

 ```yaml
 # config.<env>.yaml used in examples below
 environment:
   name: dev
   project: <insert>
   validation-type: dev
   selected:
     - modules/

 variables:
   modules:
     # stuff from quickstart package...
     organization: tx

     # ...

     cdf_ingestion:
       workflow: ingestion
       groupSourceId: <insert>
       ingestionClientId: ${IDP_CLIENT_ID}
       ingestionClientSecret: ${IDP_CLIENT_SECRET}
       pandidContextualizationFunction: contextualization_p_and_id_annotater
       contextualization_connection_writer: contextualization_connection_writer
       schemaSpace: sp_enterprise_process_industry
       schemaSpace2: cdf_cdm
       schemaSpace3: cdf_idm
       instanceSpaces:
         - springfield_instances
         - cdf_cdm_units
       runWorkflowUserIds:
         - <insert>

     contextualization:
       cdf_file_annotation:
         # used in /data_sets, /data_models, /functions, /extraction_pipelines, and /workflows
         annotationDatasetExternalId: ds_file_annotation

         # used in /data_models and /extraction_pipelines
         annotationStateExternalId: FileAnnotationState
         annotationStateInstanceSpace: sp_dat_cdf_annotation_states
         annotationStateSchemaSpace: sp_hdm #NOTE: stands for space helper data model
         annotationStateVersion: v1.0.1
         fileSchemaSpace: sp_enterprise_process_industry
         fileExternalId: txFile
         fileVersion: v1

         # used in /raw and /extraction_pipelines
         rawDb: db_file_annotation
         rawTableDocTag: annotation_documents_tags
         rawTableDocDoc: annotation_documents_docs
         rawTableCache: annotation_entities_cache

         # used in /extraction_pipelines
         extractionPipelineExternalId: ep_file_annotation
         targetEntitySchemaSpace: sp_enterprise_process_industry
         targetEntityExternalId: txEquipment
         targetEntityVersion: v1

         # used in /functions and /workflows
         launchFunctionExternalId: fn_file_annotation_launch #NOTE: if this is changed, then the folder holding the launch function must be named the same as the new external ID
         launchFunctionVersion: v1.0.0
         finalizeFunctionExternalId: fn_file_annotation_finalize #NOTE: if this is changed, then the folder holding the finalize function must be named the same as the new external ID
         finalizeFunctionVersion: v1.0.0
         functionClientId: ${IDP_CLIENT_ID}
         functionClientSecret: ${IDP_CLIENT_SECRET}

         # used in /workflows
         workflowSchedule: "*/10 * * * *"
         workflowExternalId: wf_file_annotation
         workflowVersion: v1

         # used in /auth
         groupSourceId: <insert> # source ID from Azure AD for the corresponding groups

     # ...
 ```

<video src="https://github.com/user-attachments/assets/0d85448d-b886-4ff1-96bb-415ef5efad2f"></video>

<video src="https://github.com/user-attachments/assets/9a1f1154-6d1b-4f98-bd58-cdf54e297a46"></video>

4. **Run the Workflow**

   After deployment, the annotation process is managed by a workflow that orchestrates the `Launch` and `Finalize` functions. The workflow is automatically triggered based on the schedule defined in the configuration. You can monitor the progress and logs of the functions in the CDF UI.

   - (optional) Run the ingestion workflow from the quickstart package to create instances of <org>File, <org>Asset, etc
     - Checkout the instantiated files that have been annotated using the annotation function from the quickstart package
   - (optional) Run the local_setup.ipynb to setup the files for annotation
   - Run the File Annotation Workflow

<video src="https://github.com/user-attachments/assets/1bd1b4fe-42c6-4cd7-9cde-66e51a27c8f8"></video>

<video src="https://github.com/user-attachments/assets/6a27880d-4179-4903-9112-496f7975a2eb"></video>

<video src="https://github.com/user-attachments/assets/c65980c4-b65f-4db4-8d36-765febfe65c9"></video>

### Local Development and Debugging

This template is configured for easy local execution and debugging directly within Visual Studio Code.

1.  **Create Environment File**: Before running locally, you must create a `.env` file in the root directory. This file will hold the necessary credentials and configuration for connecting to your CDF project. Populate it with the required environment variables for `IDP_CLIENT_ID`, `CDF_CLUSTER`, etc. In the `local_runs/` folder you'll find a .env template.

2.  **Use the VS Code Debugger**: The repository includes a pre-configured `local_runs/.vscode/launch.json` file. Please move the .vscode/ folder to the top level of your repo.

    - Navigate to the "Run and Debug" view in the VS Code sidebar.
    - You will see dropdown options for launching the different functions (e.g., `Launch Function`, `Finalize Function`).
    - Select the function you wish to run and click the green "Start Debugging" arrow. This will start the function on your local machine, with the debugger attached, allowing you to set breakpoints and inspect variables.
    - Feel free to change/adjust the arguments passed into the function call to point to a test_extraction_pipeline and/or change the log level.

<video src="https://github.com/user-attachments/assets/c18c05d8-2163-4301-8908-3821d5ffee48"></video>

## How It Works

The template operates in three main phases, orchestrated by CDF Workflows. Since the prepare phase is relatively small, it is bundled in with the launch phase. However, conceptually it should be treated as a separate process.

### Prepare Phase

- **Goal**: Identify files that need to be annotated or have their status reset.
- **Process**:
  1.  It queries for files that are marked for re-annotation and resets their status.
  2.  It then queries for new files tagged for annotation (e.g., with a "ToAnnotate" tag).
  3.  For each new file, it creates a corresponding `AnnotationState` instance in the data model, marking it with a "New" status.

### Launch Phase

![LaunchService](https://github.com/user-attachments/assets/3e5ba403-50bb-4f6a-a723-be8947c65ebc)

- **Goal**: Launch the annotation jobs for files that are ready.
- **Process**:
  1.  It queries for `AnnotationState` instances with a "New" or "Retry" status.
  2.  It groups these files by a primary scope to provide context.
  3.  For each group, it fetches the relevant file and target entity information, using a cache to avoid redundant lookups.
  4.  It calls the Cognite Diagram Detect API to start the annotation job.
  5.  It updates the `AnnotationState` instance with the `diagramDetectJobId` and sets the status to "Processing".

### Finalize Phase

![FinalizeService](https://github.com/user-attachments/assets/152d9eaf-afdb-46fe-9125-11430ff10bc9)

- **Goal**: Retrieve, process, and store the results of completed annotation jobs.
- **Process**:
  1.  It queries for `AnnotationState` instances with a "Processing" status.
  2.  It checks the status of the corresponding diagram detection job.
  3.  Once a job is complete, it retrieves the annotation results.
  4.  It applies the new annotations, optionally cleaning up old ones first.
  5.  It updates the `AnnotationState` status to "Annotated" or "Failed" and tags the file accordingly.
  6.  It writes a summary of the approved annotations to a CDF RAW table for reporting.

## Configuration

The templates behavior is entirely controlled by the `ep_file_annotation.config.yaml` file. This YAML file is parsed by Pydantic models in the code, ensuring a strongly typed and validated configuration.

Key configuration sections include:

- `dataModelViews`: Defines the data model views for files, annotation states, and target entities.
- `prepareFunction`: Configures the queries to find files to annotate.
- `launchFunction`: Sets parameters for the annotation job, such as batch size and entity matching properties.
- `finalizeFunction`: Defines how to process and apply the final annotations.

This file allows for deep customization. For example, you can use a list of query configurations to combine them with `OR` logic, or you can set `primaryScopeProperty` to `None` to process files that are not tied to a specific scope.

## Detailed Guides

This README provides a high-level overview of the template's purpose and architecture. To gain a deeper understanding of how to configure and extend the template, I highly recommend exploring the detailed guides located in the `cdf_file_annotation/detailed_guides/` directory:

- **`CONFIG.md`**: A document outlining the `ep_file_annotation.config.yaml` file to control the behavior of the Annotation Function.
- **`CONFIG_PATTERNS.md`**: A guide with recipes for common operational tasks, such as processing specific subsets of data, reprocessing files for debugging, and tuning performance by adjusting the configuration.
- **`DEVELOPING.md`**: A guide for developers who wish to extend the template's functionality. It details the interface-based architecture and provides a step-by-step walkthrough on how to create and integrate your own custom service implementations for specialized logic.

## Design Philosophy

There were two principles I kept in mind when designing this template.

- **Evolving Needs:** Project requirements evolve. A simple, plug-and-play tool is great to start with, but it can hit limitations when faced with demands for scale, performance, or specialized logic—as was the case with previous annotation templates when applied to projects with tens of thousands of complex files. My belief is that a modern template must be built to be extended.

- **The Balance Between Configuration and Code:** This template is architected to provide two primary modes of adaptation, striking a crucial balance:

  1.  **Quick Start (via Configuration):** For the majority of use cases, a user should only need to edit the `config.yaml` file. By defining their data model views and tuning process parameters, they can get the template running quickly and effectively.
  2.  **Scaling (via Interfaces):** When a project demands unique optimizations—like a non-standard batching strategy or a complex query to fetch entities—the interface-based design provides the necessary "escape hatch." A developer can write a custom Python class to implement their specialized logic, ensuring the template can meet any future requirement.

## Architecture & Optimizations

This section explains some of the core design choices made to ensure the template is robust and scalable.

### Stateful Processing with Data Models

Instead of using a simpler store like a RAW table to track the status of each file, this module uses a dedicated `AnnotationState` Data Model. There is a 1-to-1 relationship between a file being annotated and its corresponding `AnnotationState` instance. This architectural choice is deliberate and crucial for scalability and reliability:

- **Concurrency and Atomicity:** Data Model instances have built-in optimistic locking via the `existing_version` field. When multiple parallel functions attempt to "claim" a job, only the first one can succeed in updating the `AnnotationState` instance. All others will receive a version conflict error. This atomic, database-level locking is far more reliable and simpler to manage than building a custom locking mechanism on top of RAW.
- **Query Performance:** Finding all files that need processing (e.g., status is "New" or "Retry") is a fast, indexed query against the Data Model. Performing equivalent filtering on potentially millions of rows in a RAW table would be significantly slower and less efficient.
- **Schema Enforcement and Data Integrity:** The `AnnotationState` view enforces a strict schema for state information (`status`, `attemptCount`, `annotatedPageCount`, etc.), ensuring data consistency across the entire process. RAW tables offer no schema guarantees.
- **Discoverability and Governance:** The state of the annotation pipeline is exposed as a first-class entity in the CDF data catalog. This makes it easy to monitor progress, build dashboards, and govern the data lifecycle, which is much more difficult with state hidden away in RAW rows.

### Optimized Batch Processing & Caching

When processing tens of thousands of files, naively fetching context for each file is inefficient. This module implements a significant optimization based on experiences with large-scale projects.

- **Rationale:** For many projects, the entities relevant to a given file are often co-located within the same site or operational unit. By grouping files based on these properties before processing, we can create a highly effective cache.
- **Implementation:** The `launchFunction` configuration allows specifying a `primary_scope_property` and an optional `secondary_scope_property`. The `LaunchService` uses these properties to organize all files into ordered batches. The cache for entities is then loaded once for each context, drastically reducing the number of queries to CDF and improving overall throughput.

### Interface-Based Extensibility

The template is designed around a core set of abstract interfaces (e.g., `IDataModelService`, `ILaunchService`). This is a foundational architectural choice that enables scalability and long-term viability.

- **Contract vs. Implementation:** An interface defines a stable "contract" of _what_ a service should do. The provided `General...Service` classes offer a powerful default implementation that is driven by the configuration file.
- **Enabling Customization:** When a project's needs exceed the capabilities of the default implementation or configuration, developers can write their own concrete class that implements the interface with bespoke logic. This custom class can then be "plugged in" via the dependency injection system, without needing to modify the rest of the template's code.

## About Me

Hey everyone\! I'm Jack Zhao, the creator of this template. I want to give a huge shoutout to Thomas Molbach and Noah Karsky for providing invaluable input from a solution architect's point of view. I also want to thank Khaled Shaheen and Gayatri Babel for their help in building this.

This code is my attempt to create a standard template that 'breaks' the cycle where projects build simple tools, outgrow them, and are then forced to build a new and often hard-to-reuse solution. I genuinely believe it's impossible for a template to have long-term success if it's not built on the fundamental premise of being extended. Customer needs will evolve, and new product features will create new opportunities for optimization.
