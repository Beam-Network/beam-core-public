# BeamCore transparency — Task Assignment & PRISM

Public reference code for two parts of BeamCore: **transfer assignment** (orchestrator selection, chunk allocation, stale reassignment) and **PRISM** (scoring logic and the scheduled metrics refresh).

These files are maintained as self-contained public copies of the current BeamCore logic. This repository is not a runnable service; dependencies, scheduler wiring, and some modules are omitted or inlined.

## Files

| Path | Description |
|------|-------------|
| `packages/core-server/src/control-plane/assignment-engine.ts` | Assignment pipeline and inlined helpers used in production. |
| `packages/ops-scheduler/src/prism/scoring.ts` | PRISM score computation. |
| `packages/ops-scheduler/src/jobs/prism-score-updater.ts` | Job that aggregates DB inputs and updates PRISM metrics. |

