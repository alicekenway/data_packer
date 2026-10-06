# Parallel data packer

Create balanced, independent tar archives from a directory and recover its
contents later. Packing and unpacking support local threads and Slurm arrays.
Python 3.8 or newer is required; packing also needs GNU tar and, for compression,
gzip. There are no third-party Python runtime dependencies.

## Pack on server u

Copy `data_packer.py` and the entire `packer/` directory to the same directory on
server `u`. Run the command there, using an output directory outside `ENX`:

```bash
python3 data_packer.py pack \
  --input /mnt/users/jinyang_wang/TTS_cosyvoice/generation/ENX \
  --output /mnt/users/jinyang_wang/TTS_cosyvoice/generation/ENX_packed \
  --target-size 10GiB \
  --mode slurm \
  --partition cpu.only \
  --mem 4G --time 24:00:00 --cpus-per-task 1 \
  --concurrency 8 --wait
```

The main process scans metadata, creates the plan, prints each `sbatch` command
and job ID, and submits arrays plus a final summary job. `--wait` passes the native
`--wait` option to the final summary job's `sbatch` command. That job has an
`afterany` dependency on all arrays, so `sbatch` waits for the whole operation,
including finalization. The program then checks the summary and preserves a
nonzero `sbatch` exit code. Without `--wait`, it returns after submission.
Interrupting the wait does **not** cancel submitted jobs. Job IDs are recorded
before native waiting begins, so later retries still detect active jobs.
If the finalizer is canceled while arrays are still active, the command returns
a failure and leaves those arrays running. It does not finalize their partial
outputs locally.

Each array task creates one archive. `--concurrency` limits simultaneous tasks;
when an array must be split at the cluster limit, the split arrays run in sequence
to respect that limit across the whole operation. Each task runs one packing or
extraction worker. Additional reserved CPUs do not make GNU tar/gzip multithreaded.

Slurm resources can be specified with `--partition`, `--mem`, `--time`,
`--cpus-per-task`, `--account`, and `--qos`. If `--partition` is omitted, Slurm's
default partition is used. Additional long options are accepted as one quoted
string:

```bash
--sbatch-args='--constraint=myconstraint --exclusive'
```

Managed array/dependency/log options and conflicting resource options are
rejected. Arguments are passed directly to `sbatch`, without shell evaluation.
Use `--max-array-size N` if the cluster does not allow reading `scontrol show
config`. The program and all paths must be accessible on every participating
node, and `sbatch`, `squeue`, and `scancel` must be available.

## Local mode

```bash
python3 data_packer.py pack \
  --input /path/to/ENX --output /path/to/ENX_packed \
  --target-size 10GiB --mode local --workers 4
```

Local mode always waits. A Python thread pool runs independent GNU tar processes.
Parallelism remains limited by shared storage bandwidth; start with a modest
worker count.

Use `--parts 100` instead of `--target-size` to request an archive count. No empty
archives are generated, so the count is capped at the number of files. An empty
source is represented by the manifests without tar files.

Archives are uncompressed `.tar` by default. `--compression gzip` produces
`.tar.gz` using gzip level 1. The size target refers to estimated **uncompressed**
data plus per-file overhead, rather than compressed output. It is approximate;
files are never divided between archives, and an oversized file can exceed it.

## Metadata-first planning

The planner prefers `metadata.jsonl`, then `metadata.json`, alongside a `wav/`
directory. JSONL is streamed line by line; JSON arrays are streamed as records.
Each record needs an `audiofile_path` or `path` string. Relative audio paths are
resolved from the metadata directory. Absolute paths must point inside its
sibling `wav/` tree. Duplicate paths are included once.

Usable metadata is trusted as the complete WAV listing: the planner skips walking
that WAV tree. **Unlisted files in that tree are not packed.** Other directories
are scanned normally, including hidden directories, scripts, logs, and metadata
files themselves. Missing or unusable metadata produces a full-scan fallback;
malformed metadata emits a warning. Missing *listed* files cause an archive to
fail instead of silently disappearing.

By default, up to 128 evenly distributed files per metadata WAV directory are
checked for size. Unsampled files use the sampled mean. This speeds up planning
but yields approximate byte balancing. Use `--balance exact` to check every
listed size. The inventory lives in SQLite, and files are assigned largest-first
to the batch with the smallest estimated total. Keep source data unchanged during
planning and packing; detected source changes cause failure.

## Output and transfer

```text
ENX_packed/
  manifest.json          # configuration, outcomes and SHA-256 checksums
  inventory.sqlite3      # portable file assignments and directory metadata
  part-000000.tar
  part-000001.tar
  ...
  .work/                 # receipts, scripts, logs and submission information
```

Checksums are computed while archives are written. Files are first written to
temporary names and renamed after successful packing. A failed operation retains
successful archives and reports the failed shards.

Transfer `manifest.json`, `inventory.sqlite3`, and **every archive** together.
`.work/` is optional for restoration, but is needed to resume packing. This tool
does not upload to or download from Cloudflare.

## Recover

```bash
python3 data_packer.py unpack \
  --input /path/to/ENX_packed \
  --output /path/to/ENX_restored \
  --mode local --workers 4
```

The source directory's **contents** are restored directly: `ENX/batch_6/...`
becomes `ENX_restored/batch_6/...`. Shared and empty directories are created
before workers start. Each archive's checksum is verified before its extraction,
and directory modification times and permissions are applied after all workers
finish. Verification adds one read of each archive before extraction.

Slurm restoration accepts the same scheduler flags:

```bash
python3 data_packer.py unpack \
  --input /path/to/ENX_packed --output /path/to/ENX_restored \
  --mode slurm --partition cpu.only --mem 4G \
  --concurrency 8 --wait
```

The output must be new or empty by default. `--overwrite` intentionally replaces
conflicting regular files while retaining unrelated data; existing symlinks at
planned restore paths and directory/file type conflicts are rejected. A failed
restore can leave completed files, so retry it with `--overwrite` or use a fresh
output directory. There is no unpack-resume shortcut: retries verify and extract
the archives again.

Restore state is stored beside the output in `ENX_restored.data-packer-state/`.
`--state-dir /other/path` moves logs and receipts; the sibling directory still
holds destination ownership information to prevent overlapping restores. A state
directory must initially be empty, or be recognized as this tool's state for
the same destination. Slurm restore jobs retain ownership until the scheduler
reports that their prior jobs are inactive.

Recovery preserves contents, relative paths, ordinary permission bits,
modification timestamps, and safe relative symlinks. Ownership, ACLs, extended
attributes, privileged permission bits, and hard-link identity are not retained.
Unsafe links, archive path traversal, and special files are rejected. Metadata
file contents are copied unchanged; absolute paths inside those files are not
rewritten.

## Inspect and retry packing

```bash
python3 data_packer.py status --input /path/to/ENX_packed
python3 data_packer.py status --input /path/to/ENX_restored.data-packer-state

python3 data_packer.py pack \
  --input /path/to/ENX --output /path/to/ENX_packed --resume \
  --mode slurm --partition cpu.only --concurrency 8 --wait
```

Resume uses the saved inventory and compression settings, verifies existing
archive checksums, and retries incomplete or corrupt shards. It requires the
original source directory and refuses to overlap still-active Slurm jobs. It
does not replan changed data. If planning itself fails before the manifests are
created, use a fresh empty pack output directory after fixing the source issue.

Exit code 0 means local work succeeded, waited-for Slurm work succeeded, or
non-waiting Slurm submission succeeded. General failures return 1; a nonzero
exit code from native `sbatch --wait` is preserved. Interrupted commands return
130. Scheduler and worker logs are retained in the operation's state directory.

## Tests

```bash
python3 -m pytest -q
```

Tests use real tar archives and an executable simulated scheduler. They do not
submit jobs to server `u` or change its dataset.
