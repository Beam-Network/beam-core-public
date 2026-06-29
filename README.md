# BeamCore transparency - Task Assignment, PRISM & Weights

Public reference code for three parts of BeamCore: **transfer assignment** (orchestrator selection, chunk allocation, stale reassignment), **PRISM** (scoring logic and the scheduled metrics refresh), and **validator weights** (the final epoch materialization used by validators).

These files are maintained as self-contained public copies of the current BeamCore logic. This repository is not a runnable service; dependencies, scheduler wiring, and some modules are omitted or inlined.

## Files

| Path | Description |
|------|-------------|
| `packages/core-server/src/control-plane/assignment-engine.ts` | Assignment pipeline and inlined helpers used in production. |
| `packages/ops-scheduler/src/prism/scoring.ts` | PRISM score computation. |
| `packages/ops-scheduler/src/jobs/prism-score-updater.ts` | Job that aggregates DB inputs and updates PRISM metrics. |
| `packages/ops-scheduler/src/jobs/epoch-summary.ts` | Job that converts qualified PRISM scores and completed production tasks from the PRISM evidence window into validator weights. |
| `packages/ops-scheduler/src/jobs/epoch-summary-math.ts` | Pure raw-score and normalization helpers used by the epoch summary job. |
