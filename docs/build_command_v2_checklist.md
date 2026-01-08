# Build Command v2 - Implementation Checklist

## Phase 1: Foundation (Issues & Data Classes)

- [ ] 1.1: Create issue base classes (`BuildIssue`, `BuildError`, `BuildWarning`, `BuildHint`)
- [ ] 1.2: Create `IssueCollector` with grouping and summary printing
- [ ] 1.3: Define module issues (`MOD_001` through `MOD_006`)
- [ ] 1.4: Define variable issues (`VAR_001` through `VAR_005`)
- [ ] 1.5: Define resource issues (`RES_001` through `RES_005`)
- [ ] 1.6: Define dependency and config issues (`DEP_xxx`, `CFG_xxx`)

## Phase 2: Module Loading

- [ ] 2.1: Create Pydantic `Module` and `ResourceFolder` models
- [ ] 2.2: Create `ModuleCollection` with filtering methods
- [ ] 2.3: Implement `ModuleLoader` with comprehensive error handling
- [ ] 2.4: Migrate existing module loading to use new system

## Phase 3: Processing

- [ ] 3.1: Implement `VariableScope` and `VariableProcessor`
- [ ] 3.2: Implement variable replacement with tracking
- [ ] 3.3: Integrate with existing builders for resource processing

## Phase 4: Validation

- [ ] 4.1: Create `Validator` base class and `CompositeValidator`
- [ ] 4.2: Implement `ModuleValidator` for structure checks
- [ ] 4.3: Implement `DependencyValidator` for cross-resource deps

## Phase 5: Integration

- [ ] 5.1: Wire new build pipeline (LOAD → VALIDATE → TRANSFORM → WRITE)
- [ ] 5.2: Update CLI to use new `BuildCommand`
- [ ] 5.3: Deprecate old `build_cmd.py`, add migration path
