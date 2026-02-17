# Design Proposal: Node Respace Utility

**Feature Name:** `cdf data respace` 

**Date:** Jan 28, 2026 

**Author:** Iván Surif 

**Status:** Draft

# 1\. Problem Statement

In CDF Data Modeling architecture, a node's identity is a composite of its **Space** and **External ID**. Consequently, a node cannot simply be "moved" from one space to another. Changing the space effectively creates a new identity.

To achieve this "move," the original node must be deleted and recreated in the target space. This operation is high-risk because it breaks all dependencies, specifically:

* **Edges:** Connections to other nodes.  
* **Outgoing Direct Relations (My Data):** Properties physically stored on the node pointing to others. These must be preserved in the new node.  
* **Incoming Direct Relations (My Dependencies):** Other nodes that physically reference the node being moved. These act as foreign keys and must be updated on the *referencing* objects.  
* **Linked Resources:** Associations between **CogniteFiles and CogniteTimeSeries** with Classic Resources, which act as heavy data backends.

---

# 2\. Naming Rationale: Why respace?

I propose the command name **`respace`** to accurately reflect the operation: changing the namespace (Space) of a node.

* **Avoids Ambiguity:** The term `migrate` is already reserved in the Toolkit for moving data from the legacy "Asset-Centric" model to the new "Data Modeling" architecture.  
* **Technical Precision:** The term `respace` precisely describes the transformation—modifying the `Space` component of the identifier—without implying a simple file-system "move."  
* **External ID Invariant:** The External ID of a node is preserved during a respace. Only the Space component changes. This simplifies the operation (no need to rewrite self-references or track ID mappings) and keeps the node semantically recognizable across spaces. Supporting External ID changes may be considered in a future iteration if a use case arises.

---

# 3\. Technical Context & Strategy

### **3.1 Shared Storage (Files & Time Series)**

The design leverages the underlying architecture where CDM Nodes share storage with Asset-Centric instances.

* **Migrated Objects (Unlinkable):** For objects migrated from Asset-Centric to CDM, we **unlink** the CDM Node from the Asset-Centric instance. The CDM Node is deleted, but the data persists in the Asset-Centric instance, which is then relinked to the new Node.  
* **Native CDM Objects (Locked):** For **CogniteFiles** and **CogniteTimeSeries** objects created natively in CDM, the API prevents unlinking. These require a full **Download (Backup) → Delete → Recreate → Upload** cycle.\\

### **3.2 Relations (Dependency Direction)**

The migration strategy distinguishes between relations stored *on* the node vs. relations stored *pointing to* the node.

* **Outgoing Relations (Data Preservation):**  
  * **Definition:** The Node being respaced (`Node A`) has a physical property pointing to `Node B`.  
  * **Action:** This is data inherent to `Node A`. It is captured during download and written to the new `Node A` during recreation.  
* **Incoming Relations (Dependency Rewiring):**  
  * **Definition:** A different node (`Node C`) has a physical property pointing *to* the Node being respaced (`Node A`). This can be exposed on `Node A` via a **Reverse Direct Relation** view.  
  * **Discovery:** We query the graph using the Reverse Relation definition to find all `Node C` instances that point to `Node A`.  
  * **Action:** We must **update `Node C`** (the referencing object) to point its property to the *new* `targetSpace/ExternalID` of `Node A` (the External ID is preserved).

---

# 4\. Design Overview

**Command Structure:** `cdf data respace <subcommand> [flags]`

### **Phase 1: Planning (Discovery)**

**Command:** `cdf data respace plan`

* **Input:** CSV file with columns: `sourceSpace`, `externalId`, `targetSpace`. The External ID is preserved across spaces (see Section 2).  
* **Functionality:**  
  1. Validates source nodes exist.  
  2. Traverses the graph to identify **Dependencies**:  
     * **Edges:** Checks for edges where the node is a `startNode` or `endNode`.  
     * **Outgoing Relations:** Reads the node's properties to ensure pointers to other objects are captured.  
     * **Incoming Relations:** Queries the graph (using Reverse Relation definitions or Schema scans) to find *other nodes* pointing to the source node.  
     * **Linked Resources:** Checks for linked **CogniteTimeSeries** or **CogniteFiles** and determines if they are "Migrated" (unlinkable) or "Native" (locked).  
* **Output:** `respace_plan.json`. This plan includes a list of "Referencing Nodes" (Incoming dependencies) that require updates.

### **Phase 2: Execution (Migration)**

**Command:** `cdf data respace execute`

* **Input:** The `respace_plan.json` generated in Phase 1\.  
* **Flags:** `--backup-dir <path>` (Required)  
* **Workflow:**  
  1. **Backup:**  
     * Downloads source nodes, edges, and metadata.  
     * **Crucial:** Backs up the *Referencing Nodes* (those with incoming relations) before modification.  
     * *Pinned:* Backs up File Diagram Annotations (handled separately \- **TBD**).  
  2. **Staging (Data Preservation):**  
     * **Migrated Objects:** Unlink Classic resource from CDM node.  
     * **Native Objects:** Bulk download content/datapoints. Maybe store in CDF in temporary asset-centric objects? **TBD**  
  3. **Recreation:** Ingests new nodes into `targetSpace` (preserving Outgoing Relations).  
  4. **Pre-wiring Cleanup (Cardinality Resolution):**  
     * *Constraint:* For single-value relationships (Edges or Direct Relations), existing links must be cleared before new ones are added.  
     * *Action:* Delete specific Edges or nullify Direct Relations on Referencing Nodes to "free up" the slot.  
  5. **Rewiring & Relinking:**  
     * **Edges:** Recreate connections to the new node.  
     * **Incoming Relations:** Update the *Referencing Nodes* identified in the Plan. Change their property value from `sourceSpace/ExternalID` to `targetSpace/ExternalID` (the External ID itself is unchanged).  
     * **Classic Resources:** Relink preserved Asset-Centric instances (when these exist and had been migrated, thus persisted throughout the process)  
     * **Native Resources:** Re-upload content/datapoints.  
     * **Annotations:** Restore File Diagram Annotations (Implementation **TBD**).  
  6. **Cleanup:** Delete original nodes and remaining edges from `sourceSpace`.

---

# 5\. Implementation Roadmap

**Testing & Quality Assurance** All logic and workflows implemented across these iterations must be rigorously validated. Development will strictly adhere to the existing best practices and patterns established in the Toolkit repository, requiring:

* **Unit Tests:** To validate individual components (e.g., CSV parsing, dependency resolution, graph traversal).  
* **Integration Tests:** To verify end-to-end "Respace" workflows against a live or mocked CDF environment.  
* **Compliance:** No Pull Request (PR) will be considered "Done" without adequate test coverage matching the repository's standards.

### **Iteration 1: The Shell**

### **Scope & Objectives**

* **CLI structure:** `cdf data respace plan` / `execute`.  
* **Input:** CSV parsing and basic validation.

**Detailed Logic:**

1. **CLI Command Definition:**  
   * Register `respace` as a subcommand under the `cdf data` plugin namespace.  
   * Define `plan` (Discovery) and `execute` (Migration) subcommands.  
2. **Input Parsing (`plan`):**  
   * Implement CSV parser for headers: `sourceSpace`, `externalId`, `targetSpace`.  
   * **Validation:** Check syntax, verify source node existence (lightweight API call), and ensure `sourceSpace != targetSpace`.  
3. **Mock Plan Output:**  
   * Generate a `respace_plan.json` manifest structure without complex graph traversal.  
   * *Goal:* Verify the user can successfully define a batch and receive a structured confirmation.  
4. **Skeleton Executor (`execute`):**  
   * Load the JSON plan.  
   * Verify write access to `--backup-dir`.  
   * **No-Op Loop:** Print intended actions to `stdout` without calling writer endpoints.

---

### **Iteration 2: Simpler Entities**

### **Scope & Objectives**

* **Entities:** Implement logic for **CogniteEvents** and **CogniteAssets**.  
* **Outgoing Relations:** Detect intra-batch pointers and rewrite them.  
* **Incoming Relations:** Query external nodes pointing to the batch and update them.  
* **Edges:** Handle Edge recreation.

**Detailed Logic:**

1. **Handling Outgoing Relations (Intra-Batch Rewriter):**  
   * *Problem:* Moving a parent and child in the same batch breaks the link if simply copied (child points to old parent).  
   * *Logic:* Load the full batch into a lookup set. During creation, check every Direct Relation property. If the target is in the batch, **rewrite** the property to point to `{targetSpace, ID}`. If not, preserve as `{sourceSpace, ID}`.  
2. **Handling Incoming Relations (External Dependency Fixer):**  
   * *Problem:* External nodes (not in batch) pointing to these nodes will break.  
   * *Logic:*  
     * **Plan:** Query Views for properties targeting the batch nodes. Add these "Referencing Nodes" to the Plan.  
     * **Execute:** Download Referencing Nodes (Backup). Create an update payload changing the pointer to `{targetSpace, ID}`. Push updates *after* new nodes are created.  
3. **Handling Edges:**  
   * *Logic:* Query all edges where `startNode` or `endNode` is in the batch. Rewrite the relevant endpoint to `{targetSpace, ID}`. Recreate the Edge object. Delete the old Edge.

---

### **Iteration 3: Complex Data**

### **Scope & Objectives**

* **Entities:** Support **CogniteTimeSeries** and **CogniteFiles**.  
* **Migrated Objects:** Implement Unlink/Relink logic (Classic Staging).  
* **Native Objects:** Implement Bulk Download/Upload (Native Staging).

**Detailed Logic:**

1. **Classic Staging (Migrated):**  
   * Detect if the Node was migrated from a Classic resource.  
   * **Unlink:** Remove the link using the `unlink` endpoint.  
   * **Relink:** After node recreation, update the Classic resource with the new `assetId` (CDM Node ID).  
2. **Native Staging (Locked):**  
   * Detect if the Node is native (cannot be unlinked).  
   * **Download:** Stream datapoints/file binaries to the local backup directory (using Protobuf for Time Series).  
   * **Upload:** Re-ingest data into the new Node.

---

# 6\. Safety & Limitations

* **View Scope Only:** The tool only updates Direct Relations defined in **Views**. It does not scan raw containers for loose pointers.  
* **Unlinking Constraint:** Warning provided for Native CDM Time Series requiring full download.  
* **One Space Per Location:** Tool respects InField space constraints.

---

# 7\. Open Questions 

**1\. Staging Strategy: Local Storage vs. Cloud-Native (RAW/Files)**

**• Current Design:** Relies on local disk (--backup-dir) to store the state of nodes and edges before deletion.

**• Alternative:** Use CDF itself (e.g., temporary RAW tables or uploaded JSON files) as the staging area.

**• Trade-offs:**

**• Pros:** Removes dependency on the user’s local disk space; keeps data within the platform boundary (security/compliance); allows for centralized logging/auditing of the migration.

**• Cons:** Increases implementation complexity (requires defining/cleaning up temporary schemas); potential latency in uploading backup data vs. local writes.

**• Decision Needed:** Should we implement a "Remote Staging" mode for large-scale migrations to mitigate local hardware risks?

**2\. File Diagram Annotations (Pinned)**

**• Context:** Diagram annotations on files (PNID) are not standard Data Modeling properties but are critical for context.

**• Gap:** The current standard "Node/Edge" extraction logic does not automatically capture these.

**• Question:** What's the best approach to extract, re-map (to the new Node ID), and restore these annotations during the "Rewiring" phase?

**3\. Failure Recovery & Rollback**

**• Context:** The execute command is destructive. If the process crashes after Step 6 (Cleanup) has begun but before completion (e.g., 50% of source nodes deleted), the system is in a fractured state.

**• Question:** Do we need a dedicated cdf data respace restore command that takes the backup file and reverses the operation (recreating the deleted nodes in the source space)? Or is manual recovery via the backup file sufficient for the MVP?

**4\. Permission Scopes**

**• Context**: The tool performs broad traversals (Reverse Relations) and destructive actions (Unlink/Delete).

• Do we need to validate "Write" access to all Referencing Nodes (external dependencies) during the plan phase to prevent partial failures during execute? Should execution proceed if some nodes cannot be accessed, or halted altogether?

**5\. Reporting (Metrics & Statistics)**

**• Question:** What specific KPIs constitute a "Success Report" for this tool?

**• Volume Metrics:** Do we need to granularly report counts of Nodes, Edges, Datapoints, and Files transferred?

**• Integrity Checks:** Should the report include a "Diff" summary (e.g., "12 properties rewritten," "0 orphans detected")?

**• Persistence:** Where should this report live? 

The initial implementation will mirror the methodology established by the existing migration endpoint.

# 8\. Technical Work Plan

## Iteration 1: The Shell

### **Item 1: Define User Input Format**

#### **Step 1.1: CLI Shell Setup (Completed ✓)**

**Goal**: Establish the command structure and feature flag gating before implementation.  
What was accomplished:

| Task | Status | Details |
| :---- | :---- | :---- |
| Feature flag | ✓ | RESPACE flag in feature\_flags.py (hidden, visible=False) |
| Command registration | ✓ | RespaceApp registered under DataApp when flag enabled |
| Subcommand: plan | ✓ | Discovery phase \- analyzes nodes and generates migration manifest |
| Subcommand: execute | ✓ | Migration phase \- performs the actual respace operation |
| Placeholder output | ✓ | Both commands return "Work in Progress" message |

##### Command structure:

cdf data respace plan     \# Discovery: CSV → respace\_plan.json  
cdf data respace execute  \# Migration: respace\_plan.json → CDF operations

PR: \#2447 \- add cdf data respace command shell

#### **Step 1.2: Define CLI Parameters**

**Goal:** Specify the arguments each subcommand accepts, following existing codebase conventions.

##### Command Structure

```
cdf data respace <subcommand> [arguments] [options]
```

The `respace` command is registered under the `data` plugin and is gated by the `RESPACE` feature flag.

##### `plan` Subcommand

**Purpose:** Generate a respace plan by analyzing nodes and their dependencies (Discovery phase).

###### Usage

```
cdf data respace plan <csv_file> [--output-file <path>]
```

###### Parameters

| Parameter | Type | Position | Required | Default | Description |
| :---- | :---- | :---- | :---- | :---- | :---- |
| `csv_file` | Path | Argument | Yes | \- | Path to CSV file with nodes to respace |
| `--output-file` / `-o` | Path | Option | No | `respace_plan.json` | Path for output plan file |

###### Parameter Specifications

`csv_file` (Positional Argument)

- **Type:** File path  
- **CLI-level validation:**  
  - File must exist (`exists=True`)  
  - Must be a file, not a directory (`file_okay=True`, `dir_okay=False`)  
  - Path is resolved to absolute (`resolve_path=True`)  
- **Expected format:** CSV with columns defined in Step 1.3

`--output-file` / `-o` (Option)

- **Type:** File path  
- **Default:** `respace_plan.json` in current working directory  
- **CLI-level validation:**  
  - Parent directory must exist or be creatable  
  - Will overwrite existing file without warning  
- **Output format:** JSON (schema defined in Step 1.5)

###### Example Usage

```shell
# Basic usage with default output
cdf data respace plan nodes_to_respace.csv

# Custom output location
cdf data respace plan nodes_to_respace.csv --output-file ./plans/my_plan.json

# Using short flag
cdf data respace plan nodes_to_respace.csv -o ./plans/my_plan.json
```

###### Exit Codes

| Code | Meaning |
| :---- | :---- |
| 0 | Success \- plan generated |
| 1 | Error \- any failure (CSV parsing, validation, CDF connection) |

##### `execute` Subcommand

**Purpose:** Execute a respace plan, migrating nodes between spaces (Migration phase).

###### Usage

```
cdf data respace execute <plan_file> --backup-dir <path>
```

###### Parameters

| Parameter | Type | Position | Required | Default | Description |
| :---- | :---- | :---- | :---- | :---- | :---- |
| `plan_file` | Path | Argument | Yes | \- | Path to respace plan JSON from `plan` command |
| `--backup-dir` / `-b` | Path | Argument | Yes | \- | Directory to store backup data before migration |
| `--dry-run / -d`	 | Flag | Option | No | False | Preview changes without executing |

###### Parameter Specifications

`plan_file` (Positional Argument)

- **Type:** File path  
- **CLI Validation:**  
  - File must exist (`exists=True`)  
  - Must be a file, not a directory (`file_okay=True`, `dir_okay=False`)  
  - Path is resolved to absolute (`resolve_path=True`)

`--backup-dir` / `-b` (Positional Argument)

- **Type:** Directory path  
- **Validation:**  
  - Path syntax is valid

- **Contents after execution:**  
  - Backup files for each node (format TBD)  
  - Manifest of backed-up items


  
`--dry-run / -d (Flag)`

- **Type:** Boolean flag  
- **Default:** False  
- **Behavior:** Display what would be done without making any changes to CDF. Backup directory is still validated but no files are written.

###### Example Usage

```shell
cdf data respace execute respace_plan.json --backup-dir ./backup

# Using short flags
cdf data respace execute respace_plan.json -b ./backup

# Dry run - preview what would happen
cdf data respace execute respace_plan.json --backup-dir ./backup --dry-run
```

###### Exit Codes

| Code | Meaning |
| :---- | :---- |
| 0 | Success \- all nodes respaced |
| 1 | Error or aborted |

###### Help Text

**cdf data respace \--help**

```
Usage: cdf data respace [OPTIONS] COMMAND [ARGS]...

  Commands for respacing (moving) nodes between CDF spaces.

Options:
  --help  Show this message and exit.

Commands:
  plan     Generate a respace plan by analyzing nodes and their dependencies.
  execute  Execute a respace plan, migrating nodes between spaces.
```

**cdf data respace plan \--help**

```
Usage: cdf data respace plan [OPTIONS] CSV_FILE

  Generate a respace plan by analyzing nodes and their dependencies.

Arguments:
  CSV_FILE  Path to CSV file with nodes to respace.  [required]

Options:
  -o, --output-file PATH  Path for the output plan file.  [default: respace_plan.json]
  --help                  Show this message and exit.
```

**cdf data respace execute \--help**

```
cdf data respace execute --help

Usage: cdf data respace execute [OPTIONS] PLAN_FILE

  Execute a respace plan, migrating nodes between spaces.

Arguments:
  PLAN_FILE  Path to the respace plan JSON generated by 'plan' command.  [required]

Options:
  -b, --backup-dir PATH  Directory to store backup data before migration.  [required]
  -d, --dry-run          Preview changes without executing.
  --help                 Show this message and exit.
```

###### Design Decisions

Why is `--backup-dir` required?

- Respace is a destructive operation (deletes original nodes)  
- Backup provides recovery path if something goes wrong

Why positional arguments for primary inputs?

- Matches existing codebase patterns (`upload dir`, `modules init`)  
- More ergonomic for the common case  
- Options reserved for modifiers and flags

Why separate `plan` and `execute` commands?

- Allows review before execution  
- Plan can be version-controlled and shared  
- Supports dry-run workflows

Why does the CSV have three columns (`sourceSpace, externalId, targetSpace`) instead of four?

- The External ID is preserved during a respace — only the Space changes  
- This eliminates an entire class of errors (typos in target IDs, accidental renames)  
- It simplifies dependency rewiring: all references update only the Space component  
- If renaming External IDs becomes a requirement, it can be added as a future enhancement

