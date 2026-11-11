# bblite design notes (v0)

## 1. Thesis

Checkpoint I/O is a *publisher into a shared queue*, not a database
transaction. bblite makes that explicit: absorb into a node-local tier,
publish to the store under one atomic rename per file, and define exactly
when bytes become visible to whom. Everything else is plumbing.

## 2. Consistency model (what is guaranteed, what is a lie to avoid)

| boundary | guarantee |
|---|---|
| same process, staged file | full read-your-writes (reads/stat served from staging until drained) |
| writer calls `fsync` | bytes durable on store at final name (data `fdatasync` + `rename` + dir `fsync`) |
| writer process exit | **commit barrier**: every staged file drained or the process does not exit (bblite blocks; up to a 1 h cap, then warns) |
| other nodes, before any of the above | *no guarantee, by design.* Do not read a peer's checkpoint mid-drain. |
| node crash before writer exit | staged-only data is lost — identical to every burst-buffer and to the page cache. Durability begins at `fsync`/exit. |
| store crash mid-drain | atomic publish: final name never points to partial data (tmp+rename). A crash may leave `.bblite-tmp.*` litter, never corruption. |

The v0 commit barrier is process exit because in a checkpoint workflow the
process exit is already a cluster-wide rendezvous (allocation boundary).
Mid-run rendezvous (MPI_Barrier hook, `BBLITE_SYNC` control file, or an
MPI-aware `MPI_File_close` interpose) is roadmap item 4 — the semantics above
are deliberately conservative so every future hook *adds* visibility instead
of changing the contract.

## 3. The envelope, with measured constants

Model: `drain_capacity × Δt ≥ C` with staging headroom `S ≥ (1+h)·C`.
All rates below are from the companion ADIOS2/bbstage campaign on a 2026
Lustre deployment (8 nodes, 64 ranks, 5 GiB checkpoints) — use them as the
shape, re-measure on your system:

* **Currency matters more than width.** Buffered drain sees ~25× the rate of
  `dd oflag=direct` qd=1 at identical width (0.8 GiB/s sustained aggregate
  vs 29 MiB/s claimed, same store, same streams, same hour).
  Sizing with dd numbers is the classic self-inflicted envelope.
* **Per-stream ceiling ≈ 0.43 GiB/s** (buffered, one file, one writer
  thread; invariant to offered load ⇒ a client-path/OST limit, not
  congestion). This is why the drain is multi-lane per node and why
  per-rank demands of tens of MiB/s leave ~40× headroom at k=1.
* **Page-cache absorption ≈ 20+ GiB/s** (what "absorb at RAM speed" buys),
  with ungoverned writeback throttling as the bill.
* **NVMe SLC cliff:** 1.4–2.1 GiB/s burst → ~0.35 GiB/s sustained after a
  few GiB (measured on DRAM-less client NVMe). NVMe staging is only honest
  with admission control; tmpfs has no cliff.

Drain lane count should be auto-sized at init by probing the store with W
buffered streams (roadmap 5) — the same probe discipline, in miniature.

## 4. Semantics surface (the actual risk of the project)

Interception is trivial; meaningful POSIX is the project. v0 decisions:

| hazard | v0 answer | residual risk |
|---|---|---|
| `fsync` durability | hard drain barrier per file | low |
| `close` semantics | async drain queued | cross-node rule above |
| checkpoint commit by `rename` | staged rename + store-side follow | rename chains (`a→b→c` pre-drain) need a stress test |
| slot reuse (`O_TRUNC` on same path) | old version drained/reset first (race-tested in smoke) | staging-inode unlink on reopen race is cosmetic (space, not data) |
| `unlink` before drain | tombstone: never resurrects a deleted checkpoint | — |
| stdio (`fopen`) | intercepted + `fdopen` over staged fd; glibc-internal `__open` bypasses the `open` interpose — hence the `fopen/fopen64/fclose` trio | `freopen`, `fmemopen` oddities |
| `mmap`, `sendfile`, `copy_file_range`, `splice` | **not intercepted** — an mmap write to a staged fd writes the staging inode (fine); to the final path pre-drain (unlikely) bypasses everything | document; warn in v1 |
| `O_DIRECT` apps | passthrough + warn | none (conservative) |
| MPI-IO (ROMIO) | ROMIO's file drivers use plain `open/write/close` → likely works; aggregation changes *who* writes which files, which is semantics-neutral for bblite; **data sieving read-modify-write of a peer's undrained file would break the cross-node rule** | must test early (roadmap 1); default advice: `direct` or `romio` with `remove_empty_opaque` off... test, don't reason |
| HDF5 | POSIX driver fine; MPI_IO driver = MPI-IO case; `HDF5_USE_FILE_LOCKING` interactions with pre-drain `fcntl` (not intercepted → real flock on absent store file: test) | test |
| `fork` | child: staging disabled (no drain threads in child) | exec'd children re-init normally |
| MT writer on one fd | serialized under one global mutex; not a design target | fine for MPI ranks |

## 5. Why the exit-commit barrier is the load-bearing wall

Without a daemon, the only *unconditional* synchronization point a library
owns is its own teardown. `__attribute__((destructor))` + drain-then-join is
therefore not a convenience — it is what lets a daemonless design make
restart-correctness claims at all. Every design extension (barrier hooks,
control-file sync) reuses the same drain-and-verify machinery.

## 6. LD_PRELOAD initialization traps (v0-alpha postmortem, do not regress)

1. **`__attribute__((constructor))` runs *before* the translation unit's
   ordinary dynamic initializers.** File-scope `std::mutex`,
   `unordered_map`, `std::thread`, or any defaulted `struct Config` are
   still unconstructed when the ctor runs — and their later dynamic init
   *clobbers* everything the ctor configured (symptoms: config defaults
   print as zero, everything silently passthrough, unload hangs). Rule: all
   non-trivial state lives behind one POD pointer, initialized in a
   first-use function-local static (magic static: runtime, thread-safe).
2. **Publish the singleton *before* starting threads inside it.** A worker
   started mid-init dereferenced the not-yet-assigned global → segfault in
   `pthread_mutex_lock` on a null `this`.
3. `glibc` `open`/`open64`/`openat64`/`fopen64` are separate PLT entry
   points: interpose the whole family or you stage *some* of the app's
   writes, which is worse than staging none.
4. Private handles need `O_RDWR` even for write-only apps: the drain must
   `pread` the staged bytes (EBADF smoke failure, caught by test #1).

## 7. Experiment plan (what makes this a paper, not a repo)

1. **Baseline triangle**, same app (raw-POSIX writer or pio_bench), same
   sizes: direct-to-store | page-cache only | bblite — under (a) quiet store,
   (b) injected contention (co-tenant saturating writer), report
   per-checkpoint app-time distributions and store-side rate smoothness.
   The claim under test: same mean, *eliminated tail* + smoothed store load.
3. **vs bbstage**: engine-level staging on ADIOS2 vs bblite under the same
   workload — what does generality at the libc boundary cost/enable?
3. **Correctness suite under MPI**: restart-read-after-barrier for ADIOS2
   BP5, HDF5 (POSIX + MPI_IO), WarpX-style openPMD outputs.
4. **Envelope stress**: drive demand past `drain×Δt` and verify the
   predicted degradation: waits appear at the watermark, never as ENOSPC.
5. Envelope auto-governor: init-time width probe vs manual lane count.

## 8. Naming

`bblite` ≈ "burst buffer lite". Pre-flights before publishing: search
GitHub/PyPI/CRAN for collisions ("bblite" exists as an R package:
`bblite` reads Tableau .twb logs). It is an unrelated ecosystem (R stats),
but a paper title should disambiguate: *bblite: user-space burst buffering*
or pick `bb-lite`/`libbb-lite` if the collision feels too close for a
systems venue. Decision needed before the first arXiv submission.
