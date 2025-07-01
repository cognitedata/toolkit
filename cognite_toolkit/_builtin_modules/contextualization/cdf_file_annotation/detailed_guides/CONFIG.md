# Guide to Configuring the Annotation Function via YAML

This document outlines how to use the `ep_file_annotation.config.yaml` file to control the behavior of the Annotation Function. The Python code, particularly `ConfigService.py`, uses Pydantic models to parse this YAML, making the function adaptable to different data models and operational parameters.

## Overall Structure

The YAML configuration is organized into logical blocks that correspond to different phases and components of the toolkit:

- `dataModelViews`: Defines common Data Model views used across functions.
- `prepareFunction`: Settings for the initial file preparation phase.
- `launchFunction`: Settings for launching annotation tasks.
- `finalizeFunction`: Settings for processing and finalizing annotation results.

The entire structure is parsed into a main `Config` Pydantic model.

---

## 1. `dataModelViews`

This section specifies the Data Model views the function will interact with. Each view is defined using a structure mapping to the `ViewPropertyConfig` Pydantic model.

- **Fields for each view (in `ViewPropertyConfig`):**

  - `schemaSpace` (str): The schema space of the view (e.g., `sp_hdm`).
  - `instanceSpace` (str, optional): The data space where instances of the view are stored (e.g., `sp_dat_cdf_annotation_states`). Defaults to `None`.
  - `externalId` (str): The external ID of the view (e.g., `FileAnnotationState`).
  - `version` (str): The version of the view (e.g., `v1.0.0`).
  - `annotationType` (str, optional): For entity views, specifies the type of annotation link (e.g., `diagrams.FileLink`, `diagrams.AssetLink`). Defaults to `None`.

- **Configured Views in `ep_file_annotation.config.yaml`:**
  - `coreAnnotationView`: For storing annotation edges (e.g., `CogniteDiagramAnnotation`).
  - `annotationStateView`: For `FileAnnotationState` instances tracking file annotation progress.
  - `fileView`: For the primary files to be annotated (e.g., `CogniteFile`).
  - `targetEntitiesView`: For target entities like assets (e.g., `CogniteAsset`) to be detected. _(Pydantic model name: `target_entities_view`)_

---

## 2. `prepareFunction`

Configures the initial setup phase, primarily for selecting files to be annotated. Parsed by the `PrepareFunction` Pydantic model.

**Note:** For the query configurations below, you can provide a single query object or a list of query objects. If a list is provided, the queries are combined with a logical **OR**.

- **`getFilesForAnnotationResetQuery`** (`QueryConfig | list[QueryConfig]`, optional):

  - **Purpose:** Selects specific files to have their annotation status reset (e.g., remove "Annotated"/"AnnotationInProcess" tags) to make them eligible for re-annotation.
  - **Usage:** If present, `LaunchService.prepare()` uses this query first.
  - See [Query Configuration Details] below.

- **`getFilesToAnnotateQuery`** (`QueryConfig | list[QueryConfig]`):
  - **Purpose:** The main query to find files that are ready for the annotation process (e.g., tagged "ToAnnotate" and not "AnnotationInProcess" or "Annotated").
  - **Usage:** `LaunchService.prepare()` uses this to identify files for creating `AnnotationState` instances.

---

## 3. `launchFunction`

Settings for the main annotation job launching process. Parsed by the `LaunchFunction` Pydantic model.

- **Direct Parameters:**

  - `batchSize` (int): Max files per diagram detection API call (e.g., `50`).
  - `fileSearchProperty` (str): Property on `fileView` used for matching entities (e.g., `aliases`).
  - `targetEntitiesSearchProperty` (str): Property on `targetEntitiesView` for matching (e.g., `aliases`).
  - `primaryScopeProperty` (str, optional): File property for primary grouping/context (e.g., `site`). If set to `None` or omitted, the function processes files without a primary scope grouping. _(Pydantic field: `primary_scope_property`)_
  - `secondaryScopeProperty` (str, optional): File property for secondary grouping/context (e.g., `unit`). Defaults to `None`. _(Pydantic field: `secondary_scope_property`)_

- **`dataModelService`** (`DataModelServiceConfig`):
  **Note:** For the query configurations below, you can provide a single query object or a list of query objects. If a list is provided, the queries are combined with a logical **OR**.

  - `getFilesToProcessQuery` (`QueryConfig | list[QueryConfig]`): Selects `AnnotationState` nodes ready for launching (e.g., status "New", "Retry").
  - `getTargetEntitiesQuery` (`QueryConfig | list[QueryConfig]`): Queries entities from `targetEntitiesView` for the cache.
  - `getFileEntitiesQuery` (`QueryConfig | list[QueryConfig]`): Queries file entities from `fileView` for the cache.

- **`cacheService`** (`CacheServiceConfig`):

  - `cacheTimeLimit` (int): Cache validity in hours (e.g., `24`).
  - `rawDb` (str): RAW database for the entity cache (e.g., `db_file_annotation`).
  - `rawTableCache` (str): RAW table for the entity cache (e.g., `annotation_entities_cache`).

- **`annotationService`** (`AnnotationServiceConfig`):
  - `pageRange` (int): Parameter for creating start and end page for `FileReference`.
  - `partialMatch` (bool): Parameter for `client.diagrams.detect()`.
  - `minTokens` (int): Parameter for `client.diagrams.detect()`.
  - `diagramDetectConfig` (`DiagramDetectConfigModel`, optional): Detailed API configuration.
    - Contains fields like `connectionFlags` (`ConnectionFlagsConfig`), `customizeFuzziness` (`CustomizeFuzzinessConfig`), `readEmbeddedText`, etc.
    - The Pydantic model's `as_config()` method converts this into an SDK `DiagramDetectConfig` object.

---

## 4. `finalizeFunction`

Settings for processing completed annotation jobs. Parsed by the `FinalizeFunction` Pydantic model.

- **Direct Parameters:**

  - `cleanOldAnnotations` (bool): If `True`, deletes existing annotations before applying new ones.
  - `maxRetryAttempts` (int): Max retries for a file if processing fails.

- **`retrieveService`** (`RetrieveServiceConfig`):

  - `getJobIdQuery` (`QueryConfig`): Selects `AnnotationState` nodes whose jobs are ready for result retrieval (e.g., status "Processing", `diagramDetectJobId` exists).

- **`applyService`** (`ApplyServiceConfig`):

  - `autoApprovalThreshold` (float): Confidence score for "Approved" status.
  - `autoSuggestThreshold` (float): Confidence score for "Suggested" status.

- **`reportService`** (`ReportServiceConfig`):
  - `rawDb` (str): RAW DB for reports.
  - `rawTableDocTag` (str): RAW table for document-tag links.
  - `rawTableDocDoc` (str): RAW table for document-document links.
  - `rawBatchSize` (int): Rows to batch before writing to RAW.

---

## Query Configuration Details (`QueryConfig`)

Used by various services to define data model queries. Parsed by `QueryConfig` Pydantic model.

- **Query Logic:**

  - **AND Logic**: Within a single `QueryConfig` block, all conditions listed under the `filters` key are combined with a logical **AND**.
  - **OR Logic**: For query fields (like `getFilesToAnnotateQuery`), you can provide a YAML list of `QueryConfig` blocks. These will be combined with a logical **OR**, allowing you to select items that match _any_ of the provided query blocks.

- **`targetView`** (`ViewPropertyConfig`): Specifies the view to query against. See [dataModelViews].

- **`filters`** (list of `FilterConfig`): A list of conditions that are **ANDed** together. Each condition is a `FilterConfig` object:

  - `values` (str, list of str, `AnnotationStatus` Enum/list, optional): Value(s) for the filter. Can be `AnnotationStatus` Enum members (e.g., "New", "Processing") or plain strings/numbers. Omit for `Exists`.
  - `negate` (bool, default `False`): If `True`, inverts the condition (e.g., NOT IN).
  - `operator` (`FilterOperator` Enum): The comparison type (e.g., "In", "Equals", "Exists"). Values from `utils.DataStructures.FilterOperator`.
  - `targetProperty` (str): The property in `targetView` to filter on (e.g., "tags", "annotationStatus").

- **`limit`** (Optional[int], default `-1`): Specifies the upper limit of instances that can be retrieved from the query.

The Python code uses `QueryConfig.build_filter()` (which internally uses `FilterConfig.as_filter()`) to convert these YAML definitions into Cognite SDK `Filter` objects for querying CDF.
