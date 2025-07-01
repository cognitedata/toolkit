# Annotation Template: Configuration Recipes and Usage Patterns

This guide provides practical examples and common usage patterns for the `extraction_pipeline_config.yaml` file. It is intended for users who are familiar with the overall architecture of the Annotation template and want to tailor its behavior for specific tasks.

Each section below represents a "recipe" for achieving a specific goal by modifying the configuration. For consistency, all recipes will use the following example:

- **Data Model Space:** `sp_tx`
- **View Name:** `txFile`
- **Instance Spaces:** `sp_dat_files_austin`, `sp_dat_files_houston`
- **City:** Austin (`ATX`), Houston (`HOU`)
- **File External IDs:** `pid-aus-42-001`, `pid-hou-13-005`

## Common Configuration Patterns

This section covers common, granular adjustments you can make to the configuration to control the day-to-day behavior of the annotation function.

### Recipe 1: Processing a Specific Subset of Files

**Goal:** Run the template on a very specific subset of your data, such as files belonging to a particular city and county. This is useful for targeted rollouts or testing.

**Scenario:** Only process files where `city` is "ATX" (Austin) and `county` is "travis".

**Configuration:**
Modify the `filters` list within the `prepareFunction.getFilesToAnnotateQuery`. You will add two new `Equals` filters to the standard ones.

```yaml
# In ep_file_annotation.config.yaml

prepareFunction:
  getFilesToAnnotateQuery:
    # ... (targetView details)
    filters:
      # This is the new filter for the city
      - values: "ATX"
        negate: False
        operator: Equals
        targetProperty: city
      # This is the new filter for the county
      - values: "travis"
        negate: False
        operator: Equals
        targetProperty: county
      # This standard filter ensures we only pick up files ready for annotation
      - values: ["ToAnnotate"]
        negate: False
        operator: In
        targetProperty: tags
      # This standard filter excludes files already in the process
      - values: ["AnnotationInProcess", "Annotated", "AnnotationFailed"]
        negate: True
        operator: In
        targetProperty: tags
```

### Recipe 2: Reprocessing Specific Files for Debugging

**Goal:** Force reprocessing for one or more files that were processed incorrectly or that you want to re-run for testing purposes.

**Scenario:** Force reprocessing for two specific files: `pid-aus-42-001` and `pid-hou-13-005`.

**Configuration:**
Un-comment and modify the `getFilesForAnnotationResetQuery` in the `prepareFunction` section. This query runs before the main `getFilesToAnnotateQuery`.

```yaml
# In ep_file_annotation.config.yaml

prepareFunction:
  # Un-comment this entire section to activate it
  getFilesForAnnotationResetQuery:
    targetView:
      schemaSpace: "sp_tx"
      externalId: "txFile"
      version: "v1"
    filters:
      - values: ["pid-aus-42-001", "pid-hou-13-005"] # List the external IDs
        negate: False
        operator: In
        targetProperty: externalId

  getFilesToAnnotateQuery:
    # ... (this query remains as is)
```

### Recipe 3: Adjusting the Processing and Caching Strategy

**Goal:** Change how files are grouped for processing to optimize cache performance, especially if your data model uses different property names for city and county.

**Scenario:** Your data model uses `street` instead of `city` for the primary grouping, and you have no secondary grouping property.

**Configuration:**
Modify the `primaryScopeProperty` and `secondaryScopeProperty` keys in the `launchFunction` section.

```yaml
# In ep_file_annotation.config.yaml

launchFunction:
  primaryScopeProperty: "street" # Changed from city
  # secondaryScopeProperty: "county"     # Omit or comment out
  # ... (rest of launchFunction config)
```

### Recipe 4: Fine-Tuning the Diagram Detection API

**Goal:** Adjust the behavior of the diagram detection model, for example, by making it more or less strict about fuzzy text matching.

**Scenario:** Increase the minimum confidence score for fuzzy matches to 0.9 to reduce the number of incorrect low-confidence matches.

**Configuration:**
Modify the parameters inside `launchFunction.annotationService.diagramDetectConfig`.

```yaml
# In ep_file_annotation.config.yaml

launchFunction:
  # ... (other configs)
  annotationService:
    diagramDetectConfig:
      minFuzzyScore: 0.90 # Changed from 0.85
      # ... (other DiagramDetectConfig properties)
```

### Recipe 5: Combining Queries with OR Logic

**Goal:** To select files for processing that meet one of several distinct criteria. This is useful when you want to combine different sets of filters with a logical OR.

**Scenario:** You want to annotate all files that are either in the "Austin" (`ATX`) city and have the "ToAnnotate" tag OR are in the "Houston" (`HOU`) city, belong to "Fort Bend", and have the "ToAnnotate" tag.

**Configuration:**
Instead of a single query configuration, you can provide a list of `QueryConfig` objects. The system will automatically combine them with an `OR` operator.

```yaml
# In ep_file_annotation.config.yaml

prepareFunction:
  getFilesToAnnotateQuery:
    - targetView:
        schemaSpace: "sp_tx"
        externalId: "txFile"
        version: "v1"
      filters:
        - values: "ATX"
          negate: False
          operator: Equals
          targetProperty: city
        - values: ["ToAnnotate"]
          negate: False
          operator: In
          targetProperty: tags
    - targetView:
        schemaSpace: "sp_tx"
        externalId: "txFile"
        version: "v1"
      filters:
        - values: "HOU"
          negate: False
          operator: Equals
          targetProperty: city
        - values: "Fort Bend"
          negate: False
          operator: Equals
          targetProperty: county
        - values: ["ToAnnotate"]
          negate: False
          operator: In
          targetProperty: tags
```

### Recipe 6: Annotating Files Without a Scope

**Goal:** To annotate files that do not have a `primaryScopeProperty` (e.g., `city`). This is useful for processing files that are not assigned to a specific city or for a global-level annotation process.

**Scenario:** You want to annotate all files that have the "ToAnnotate" tag but do not have a value for the `city` property.

**Configuration:**
In the `launchFunction` configuration, set the `primaryScopeProperty` to `None`. The system will then process all files that match the `getFilesToProcessQuery` without grouping them by a primary scope.

```yaml
# In ep_file_annotation.config.yaml

launchFunction:
  primaryScopeProperty: None # Set to null or remove the line entirely
  # ... (rest of launchFunction config)
  dataModelService:
    # ...
    getTargetEntitiesQuery:
      # ... (filters for entities that are global or have no city)
```

## Architectural Patterns: Scoping Data

This section covers high-level architectural decisions about how the template finds and partitions data. The choice between these patterns is fundamental and depends on your organization's requirements for governance, security, and operational structure.

### Recipe 7: Global Scoping (Searching Across All Spaces)

**Goal:** To run a single, unified annotation process that finds and annotates all new files based on their properties, regardless of which physical `instanceSpace` they reside in.

**Scenario:** A central engineering team is responsible for annotating all new P\&IDs across the entire organization, including from the Austin (`ATX`) and Houston (`HOU`) cities. You want a single, global workflow to find all files tagged `ToAnnotate`.

**Configuration:**
In your configuration file, **omit or comment out** the `instanceSpace` property specifically within the **`dataModelViews.fileView`** section.

```yaml
# In ep_file_annotation.config.yaml for a global annotation workflow

dataModelViews:
  fileView:
    schemaSpace: "sp_tx"
    # instanceSpace: "sp_dat_files_austin"  <- This line is omitted for a global query
    externalId: "txFile"
    version: "v1"
```

#### **Pros & Cons for Annotation**

- **Pros:** A single workflow can handle annotating all new files across the enterprise, which is simpler to manage for a central team.
- **Cons:** It's not possible to apply different annotation rules per city within this single workflow. The initial query can be slower on very large systems.

#### **When to Choose this Method**

- When a single team uses a single, consistent set of rules to annotate all files across the organization.
- For simpler systems where strict data partitioning between different domains is not a requirement.

### Recipe 8: Isolated Scoping (Targeting a Specific Space)

**Goal:** To run a dedicated annotation process that operates only within a single, physically separate data partition.

**Scenario:** Your organization requires that data from the Austin city be kept separate. The annotation workflow for Austin must _only_ find and process files tagged `ToAnnotate` that physically reside in the `sp_dat_files_austin` instance space.

**Configuration:**
Explicitly set the `instanceSpace` property specifically within the **`dataModelViews.fileView`** section of your configuration file.

```yaml
# In ep_file_annotation.config.yaml for the Austin workflow

dataModelViews:
  fileView:
    schemaSpace: "sp_tx" # The central "blueprint"
    instanceSpace: "sp_dat_files_austin" # Pointing to the specific instance space
    externalId: "txFile"
    version: "v1"
```

#### **Pros & Cons for Annotation**

- **Pros:** Guarantees strong security and isolation. Allows for different annotation rules for each data partition. Queries are faster.
- **Cons:** Requires separate configurations and workflows for each instance space. Ingestion pipelines must route data to the correct space.

#### **When to Choose this Method**

- When data governance, security, and audibility are primary concerns.
- When you need to apply different annotation logic or rules to different datasets.

### **Operationalizing the Isolated Scoping Pattern**

To use the recommended "Isolated Scoping" pattern to annotate files across your entire system, you must adopt a multi-workflow approach:

1.  **Create Separate Config Files:** For each `instanceSpace` (e.g., `sp_dat_files_austin`, `sp_dat_files_houston`), create a dedicated configuration file (e.g., `ep_file_annotation_austin.config.yaml`, `ep_file_annotation_houston.config.yaml`).
2.  **Create Separate Extraction Pipelines:** In Cognite Data Fusion, create a corresponding `ExtractionPipeline` resource for each config file.
3.  **Create and Trigger Separate Workflows:** Define separate workflows, one for each city. The Austin workflow would be triggered with the Austin extraction pipeline external ID, and the Houston workflow with its respective pipeline ID.

This ensures that each annotation process runs in its own isolated, controlled, and configurable environment.
