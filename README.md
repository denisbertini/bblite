# bblite — burst buffer, lite

Daemonless, 100% user-space I/O staging that shields HPC simulations *and*
the parallel filesystem from checkpoint jitter. `LD_PRELOAD` a single shared
library; declared checkpoint files spill to a node-local staging tier at RAM
speed and drain to the shared store (Lustre, ...) asynchronously, paced and
watermarked. Zero root, zero daemons, zero cluster configuration, zero
application changes — works under any I/O stack (ADIOS2, HDF5, openPMD,
raw POSIX).

**Status: v0 skeleton.** The staging engine, drain scheduler, watermark,
commit barrier, and the POSIX semantics surface below exist and pass a
correctness smoke suite on Linux/x86-64. It is not yet benchmarked, not MPI
validated, and not production-ready. See [DESIGN.md](DESIGN.md) for the
consistency model and open problems.

## Why (and why not just the page cache?)

When a simulation checkpoints into a contended Lustre, jitter stalls every
GPU node at the next barrier. Vendor burst buffers fix this with distributed
filesystems and privileged daemons. bblite fixes it with the 300 lines of
buffer logic that the kernel's page cache *almost* already gives you — but
without its surprises:

* **Predictable absorption.** The page cache absorbs checkpoints at ~20 GiB/s
  (measured) — and then the kernel decides *when* and *how hard* the writeback
  throttle lands on your next write. bblite absorbs into its own staging tier
  and drains on a schedule it controls: `drain_rate × Δt ≥ checkpoint_size`.
* **Metadata flattening.** One file per checkpoint appears on the store via
  tmp-write + `rename` + directory `fsync` — no partially-written files for
  lfs/scanners/quota daemons to trip over, no violent create/extend bursts.
* **Jitter insulation with an explicit degradation curve.** A store slowdown
  lengthens the drain, the staging watermark fills, and backpressure arrives
  as bounded, visible writer stalls — not as a 300 s kernel freeze on an
  unrelated write. The envelope is observable; that is the product.
* **Honest supply numbers.** A buffered drain lane reaches the store's
  buffered admission rate (measured ~0.4 GiB/s per file stream — a per-stream
  client-path ceiling; lanes are wide by design). Do not size the envelope
  with `dd oflag=direct`: O_DIRECT qd=1 measures a floor ~25× below what the
  engine currency actually gets (all figures from companion measurements,
  see DESIGN.md).
* **Staging tier: RAM first.** `/dev/shm`-class tmpfs is the primary absorb
  tier: flat speed, no cliff. Consumer/enterprise NVMe looks faster until the
  SLC cache exhausts (~2 GiB/s burst → ~0.35 GiB/s sustained at a few GiB
  written, measured) — *below* some Lustre shares, so a second checkpoint can
  land on the cliff and make things worse. NVMe support is on the roadmap and
  gated on SLC-aware admission; until then, size staging to `k × checkpoint`.

## Quick start

```bash
make                       # builds libbblite.so (no MPI, no deps beyond libc/pthread)
make test                  # correctness smoke suite

# wrap any application: store=/path/to/parallel/fs  staging=fast local dir
export BBLITE=1
export BBLITE_STORE=/lustre/rz/alice/simout
export BBLITE_STAGING_DIR=/dev/shm/bblite
export BBLITE_PATTERN='chkpt* checkpoint*'
LD_PRELOAD=/path/to/libbblite.so mpirun -np 400 ./solver inputs
```

Nothing matches → nothing changes: with `BBLITE` unset, or a file outside
`BBLITE_STORE` or not matching `BBLITE_PATTERN`, every call is a passthrough.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `BBLITE` | `0` | Master switch. `0`/unset = 100% passthrough. |
| `BBLITE_STORE` | — | Absolute store prefix. Only files under it can be staged. |
| `BBLITE_STAGING_DIR` | — | Node-local staging root (tmpfs recommended). Layout mirrored store-relative. |
| `BBLITE_PATTERN` | — | Space-separated globs matched against file **basename**. Declares what is a checkpoint. |
| `BBLITE_WATERMARK_PCT` | `80` | Staging-fs usage (%) at which new checkpoint opens wait for the drain (bounded ~30 s, then proceed with warning — never deadlocks). |
| `BBLITE_DRAIN_LANES` | `4` | Background drain threads. Per-lane throughput is store-per-stream-bound (~0.4 GiB/s measured); width is how you scale. |
| `BBLITE_VERBOSE` | `0` | Log staging/drain decisions to stderr. |

## Semantics contract (v0)

| Call | Staged file behavior |
|---|---|
| `open` (+write) | Redirects to staging; a missing parent dir is auto-mirrored. |
| `write/pwrite/lseek/read` | Operate on the staging copy; reads see your latest bytes. |
| `close` | Returns after the local spill; drain queued **per file** (async point). |
| `fsync/fdatasync` | Real durability barrier: blocks until bytes are `fdatasync`'d on the store, `rename`d into place, dir `fsync`'d. |
| `open(O_RDONLY)`, `stat` | Served from staging while undrained → you can open-your-own-checkpoint immediately. |
| `rename` | Staged copy renamed + store-side name follows the drain. Rename *out of* the store: passthrough + warning. |
| `unlink` | Staged copy dropped, drain tombstoned, store-side unlinked if already drained. |
| process exit | **Commit barrier:** all outstanding drains finish before exit. Restart-after-job-end is always correct. |
| `O_DIRECT`, `O_TMPFILE`, dirfd-relative opens, `fork` children | Passthrough (warned once / disabled in child). |

**Cross-node rule:** *another rank/job must never read a file while its
writer's drain is pending.* Same-process visibility is complete; cluster
visibility begins at each writer's exit (or its explicit `fsync`). For
mid-run consumers, `fsync` on the writer side is the contract.

## Positioning

Adjacent prior art, all of which bblite must be measured against:
**IOFive** (LLNL; LD_PRELOAD log-structured staging — the closest ancestor),
**OverWrite** (IBM, content-addressed write staging), **IronHide**
(daemonless metadata staging), vendor burst buffers (DDN LDMA, NexGen
Cloudian; DAOS as the new-stack alternative), and the implicit baseline:
**the page cache** (see above — it is fast but ungoverned).

bblite's differentiators are the *governor*, not the intercept: measured-currency
drain scheduling, explicit degradation semantics, and the exit-commit
correctness model in ~zero-dependency user space. Its big sibling
[bbstage](https://github.com/your-org/bbstage) does the same architecture
*inside* ADIOS2; bblite generalizes it below the framework, at the cost of
the POSIX semantics surface above.

## Roadmap

1. MPI validation matrix: ROMIO/MPI-IO (aggregation + data sieving), HDF5
   POSIX vs MPI_IO drivers, ADIOS2 BP5 raw-POSIX path.
2. The money experiment: same job with/without bblite under injected store
   contention (co-tenant load), app-time distribution + store-side smoothing.
3. NVMe overflow tier with SLC-aware admission; staging capacity governor
   tied to the measured-envelope model.
4. Multi-rank commit rendezvous (visibility at MPI barrier, not only exit).
5. Direct-vs-buffered drain probe per store at init (auto-size lanes).
