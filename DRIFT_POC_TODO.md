# Drift Detection & Update Plugin (POC - Transformations)

## Goal

- Build a drift detection and update plugin to reduce differences between UI/CDF data and local Toolkit configs.
- Scope for POC: transformations only.

## Outcomes (POC)

- Detect three cases for transformations:
  - Local-only: exists in Toolkit project, not in UI. Suggest deploy.
  - UI-only: exists in UI, not in Toolkit. Prompt user to choose module, dump YAML.
  - Both: show diff; user picks local or UI; apply choice.

## High-Level Flow

1) Build local modules to get rendered configs.
2) Collect local transformation definitions (by external_id).
3) Fetch UI transformations and map by external_id.
4) Bucketize: local-only, ui-only, both.
5) For ui-only, select/create module and dump YAML.
6) For both, show diff and apply user choice.
7) For local-only, suggest build+deploy.

## Team Split (3 hours)

- Aashutosh: plugin wiring, CLI, orchestration skeleton, reuse of diff utilities.
- Charan: UI-only path (module select/create, dump); local-only deploy suggestion.
- Anju: both-present diff and apply; flags/safety.

## Detailed TODO (with file references)

1) Register new plugin and wire CLI
   - Edit `cognite_toolkit/_cdf_tk/plugins.py`: add `drift = Plugin("drift", "Drift detection and update plugin")` to `Plugins`.
   - Edit `cognite_toolkit/_cdf.py`: if `Plugins.drift.value.is_enabled(): _app.add_typer(DriftApp(**default_typer_kws),
      name="drift")` and import `DriftApp`.

2) Create CLI app
   - New `cognite_toolkit/_cdf_tk/apps/_drift.py` with `class DriftApp(typer.Typer)`.
   - Command: `cdf drift transformations` with options `--organization-dir`, `--env`, `--dry-run`, `--yes`,  
     `--verbose`.
   - In handler: create `DriftCommand` and call `run_transformations(...)`.

3) Implement orchestrator command
   - New `cognite_toolkit/_cdf_tk/commands/drift.py` with `class DriftCommand(ToolkitCommand)` and  
     `run_transformations(...)`.
   - Build local using `BuildCommand.execute(...)` into a temp `build_dir` (see `PullCommand.pull_module`).
   - Enumerate local transformations: reuse `PullCommand._get_local_resource_dict_by_id` logic.
   - Fetch UI transformations using `TransformationCRUD` or `TransformationFinder` and map by external_id.
   - Bucketize into local-only, ui-only, both.

4) Build local modules and collect IDs
   - Use `BuildCommand` like `PullCommand.pull_module` to produce `BuiltModuleList`.
   - From resources, collect transformation identifiers and YAML dicts.

5) Fetch UI transformations
   - Use `TransformationCRUD(client, build_dir=None)` to list/retrieve.
   - Alternatively use `TransformationFinder` from `dump_resource.py` and update internal map.

6) Bucketize resources (pure function)
   - Input: `local_by_id`, `ui_by_id`.
   - Output: `local_only`, `ui_only`, `both`.

7) UI-only path: interactive module selection/create
   - Enumerate modules with `ModuleDirectories.load(organization_dir, None)` and prompt via  
     `questionary.select`.
   - Option to "Create new module": create directory and `transformations/` subfolder.

8) Dump UI-only transformations to chosen module
   - Use `DumpResourceCommand` patterns to write YAML via `TransformationCRUD.dump_resource` and  
     `split_resource`.
   - Path: `<module>/transformations/<external_id>.Transformation.yaml`.

9) Both-present: show diffs
   - Generate CDF-dumped dict using `loader.dump_resource(cdf_resource, local_dict)`.
   - Use `ResourceYAMLDifference` and `TextFileDifference` from  
     `cognite_toolkit/_cdf_tk/commands/pull.py` to display.

10) Apply user choice (keep local or UI)
    - Keep local: no change.
    - Use UI: backup file and overwrite. Reuse `ResourceReplacer` and `PullCommand._update` flow to  
      preserve variables/comments.

11) Local-only: suggest deploy
    - Offer build+deploy path for the relevant module(s) using `BuildCommand` then  
      `DeployCommand.deploy_build_directory(...)` with `include=["transformations"]`.

12) Flags & safety
    - `--dry-run`: no writes.
    - `--yes`: auto-accept defaults (suggest no-op for local-only by default unless specified; adjust per  
      team decision).
    - Backups: create `<file>.bak` on overwrite.

13) Minimal tests
    - Unit test for bucketize function.
    - Unit test for apply-choice path (no-op vs overwrite decision logic).

14) Docs (this file) and cdf.toml snippet
    - Add usage and plugin enablement snippet below.

## Concrete File Edits / Creation

- Edit: `cognite_toolkit/_cdf_tk/plugins.py` (add `drift`).
- Edit: `cognite_toolkit/_cdf.py` (wire `DriftApp` under plugin toggle).
- New: `cognite_toolkit/_cdf_tk/apps/_drift.py` (Typer app and command entrypoint).
- New: `cognite_toolkit/_cdf_tk/commands/drift.py` (orchestrator + helpers).

## Key Code Reuse

- Module selection: `cognite_toolkit/_cdf_tk/commands/pull.py` (`pull_module`, `ModuleDirectories`).
- Diff/replace: `ResourceYAMLDifference`, `TextFileDifference`, `ResourceReplacer` in `pull.py`.
- Transformations CRUD: `cognite_toolkit/_cdf_tk/cruds/_resource_cruds/transformation.py`.
- YAML model: `cognite_toolkit/_cdf_tk/resource_classes/transformations.py`.
- Dump logic: `cognite_toolkit/_cdf_tk/commands/dump_resource.py` (`TransformationFinder`, `DumpResourceCommand`).

## Enable Plugin (cdf.toml)

```toml
[plugins]
drift = true
```

## Usage (POC)

```bash
cdf drift transformations --organization-dir . --env dev --dry-run
cdf drift transformations --yes
```

## Suggested Assignments & Time

- Aashutosh: Tasks 1–3. ~1h40
- Charan: Tasks 7–8 and 11. ~1h30
- Anju: Tasks 9–10 and 12. ~1h45
- All: Task 13–14. ~30–40m shared

## Demo Flow

1) Enable plugin in `cdf.toml`.
2) Run dry-run to preview diffs and actions.
3) Accept actions interactively or with `--yes`.
4) Verify updated files and/or deployed resources.
