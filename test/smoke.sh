#!/usr/bin/env bash
# v0 smoke: correctness of staging / drain / commit semantics through the shim.
# Run: make test   (or: bash test/smoke.sh after make)
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f libbblite.so ] || { echo "build first: make" >&2; exit 1; }
[ -x writer ] || make writer >/dev/null

ROOT=$(mktemp -d "${TMPDIR:-/tmp}/bblite-smoke.XXXXXX")
trap 'rm -rf "$ROOT"' EXIT
STORE=$ROOT/store STAGE=$ROOT/stage
mkdir -p "$STORE" "$STAGE"
export BBLITE=1 BBLITE_STORE="$STORE" BBLITE_STAGING_DIR="$STAGE" \
       BBLITE_PATTERN='chkpt*' BBLITE_WATERMARK_PCT=95 BBLITE_DRAIN_LANES=2 \
       BBLITE_VERBOSE=${V:-0}
P="env LD_PRELOAD=$PWD/libbblite.so"
fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

# 1. plain spill: after process exit the file is on the store, correct,
#    and staging was reclaimed (exit = commit barrier)
$P ./writer plain "$STORE/chkpt_000000" 64
[ -f "$STORE/chkpt_000000" ]           || fail "chkpt not on store after exit"
[ "$(stat -c%s "$STORE/chkpt_000000")" = $((64*1024*1024)) ] || fail "chkpt size wrong"
./writer verify "$STORE/chkpt_000000" 64 >/dev/null || fail "content mismatch"
[ -z "$(ls -A "$STAGE")" ]             || fail "staging not reclaimed at exit"

# 2. stdio path (fopen/fwrite/fclose)
$P ./writer stdio "$STORE/chkpt_stdio" 16
./writer verify "$STORE/chkpt_stdio" 16 >/dev/null || fail "stdio content mismatch"

# 3. fsync durability barrier
$P ./writer fsync "$STORE/chkpt_fsync" 16
./writer verify "$STORE/chkpt_fsync" 16 >/dev/null || fail "fsync content mismatch"

# 4. slot reuse: rewrite same path 3x within one process (O_TRUNC must not
#    mix checkpoint versions)
$P ./writer rewrite "$STORE/chkpt_reuse" 8
[ "$(stat -c%s "$STORE/chkpt_reuse")" = $((8*1024*1024)) ] || fail "reuse: wrong size"
./writer verify "$STORE/chkpt_reuse" 8 >/dev/null || fail "reuse: content mismatch"

# 5. non-matching pattern passes through untouched
$P ./writer plain "$STORE/other_data" 4
[ "$(stat -c%s "$STORE/other_data")" = $((4*1024*1024)) ] || fail "passthrough write broken"

# 6. ten-checkpoint cadence soak
for i in $(seq 1 10); do $P ./writer plain "$STORE/chkpt_loop_$i" 4; done
for i in $(seq 1 10); do
    ./writer verify "$STORE/chkpt_loop_$i" 4 >/dev/null || fail "loop $i mismatch"
done

# 7. disabled = zero behavior change
BBLITE=0 $P ./writer plain "$STORE/chkpt_off" 4
./writer verify "$STORE/chkpt_off" 4 >/dev/null || fail "BBLITE=0 broken"

echo "SMOKE OK"
