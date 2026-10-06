# Implementation record

Approved scope: metadata-first balanced tar packing and safe recovery, local
threads or Slurm arrays, configurable resources and optional waiting. No upload.

Tasks:
- [x] Inventory and local pack/unpack, proven with actual tar round trips.
- [x] Slurm scheduling and waiting, proven with an executable fake scheduler.
- [x] Failure handling, resume, security review, documentation and final checks.

Ruling: implement in the requested directory; it has no usable Git repository,
so worktree creation and commit bookkeeping do not apply.
Ruling: canonical metadata.jsonl and metadata.json are trusted listings; unrelated
JSON files remain ordinary data. Inventory is portable SQLite; manifest.json
records archive outcomes and SHA-256 checksums. Directory metadata is applied
only after every extraction worker finishes.

Review fixes verified with regression tests:
- Effective symlink resolution, including chains, is checked during packing and
  recovery. Final safety validation includes partial/failed workers and removes
  every detected escaping link; records are flushed before links are installed.
- Restore state requires a recognized ownership marker. Destination ownership
  applies independently of a custom state directory and persists for async jobs.
- Exact planning uses running aggregates; parent caches are bounded. New restore
  preflight avoids per-file filesystem checks for an empty destination.
- PAX modification timestamps are restored with nanosecond precision.
- Queue monitoring handles purged completed IDs by filtering the user's queue.
- Finalizer failures are recorded in summary.json; status uses finalized outcomes.
- Missing listed parent directories fail planning rather than dropping paths in
  a scan fallback. Packing rechecks output emptiness under its coordinator lock.

Ruling: memory regression measures planner allocations in an isolated process,
excluding Python's environment-dependent pathlib intern table and pytest plugins.
Ruling: ordinary permission bits and modification times are preserved; ownership,
ACLs, extended attributes, special permission bits and hard-link identity are not.

Validation before the native-wait correction: full suite 42 tests passed, including a 50,000-entry exact-planning
memory regression, real tar round trips, and executable fake Slurm jobs. All seven
runtime Python files parse with the Python 3.8 grammar and compile. CLI help checked.
Independent focused re-review found no remaining Important/Critical finalizer
issues after its regression tests passed. No production Slurm jobs submitted.

User clarification: --wait is specifically the sbatch option. Removed queue
polling for waiting; pass native --wait on the finalizer that depends on all
arrays. Preserve native job failure exit codes without treating them as rejected
submissions. Capture/persist the accepted job ID before blocking, and retain
ownership records on interruption. The initial --poll-interval remains accepted
for compatibility but is hidden and unused. Queue queries remain only for
preventing overlaps with previous jobs.

Native wait regression coverage: actual flag forwarding, no queue polling,
preserved nonzero job status, distinction between rejection/execution failure,
missing finalizer recovery, ID persistence before blocking and interruption.
Recovery of a missing finalizer summary is guarded by a one-time queue check:
a canceled dependent finalizer can finish before its arrays. Active/unknown
array state blocks local finalization, and native nonzero exit codes are retained
even when the summary is missing.

Final native-wait validation: all 49 tests passed; all runtime files passed
Python 3.8 grammar and compilation checks. Focused independent re-review found
no remaining Important guard/status issues. Scheduler tests use a simulator;
no production Slurm jobs were submitted.
