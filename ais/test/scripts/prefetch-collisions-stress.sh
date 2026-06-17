#!/usr/bin/env bash

# Concurrent prefetch of overlapping object names, stressing the respective collision paths.
#
# Tip: use a cloud bucket, optionally with a prefix, with up to 10K small objects.
#
# Mechanism under test: repeatedly evict and concurrently prefetch overlapping
# remote object names. With --keep-md, local metadata remains available while
# object payloads are cold, increasing the chance that multiple workers collide
# on the same object metadata/cache path.
#
# Detection:
# - target panic / fatal "concurrent map writes" should fail in-flight prefetch;
# - cluster health must remain Operational with stable target membership;
# - optional --log-dir scans fresh/current logs for race/panic signatures.
#
# Usage:
# ./prefetch-collisions-stress.sh --bucket s3://ais-vm [--prefix a] \
# [--rounds N] [--par N] [--workers N] [--stagger-ms N] \
# [--blob-threshold SIZE] [--no-keep-md] [--log-dir DIR] [-v]
#
# Exit:
# 0 = all rounds completed cleanly
# non-zero = failed prefetch, unhealthy cluster, or optional log scan hit

set -euo pipefail

# ----- defaults -----

BCK=""
PREFIX="${PREFIX:-}"
ROUNDS="${ROUNDS:-10}"
PAR="${PAR:-8}"                      # concurrent prefetch jobs per round
WORKERS="${WORKERS:-64}"             # per-job num-workers; server may clamp
STAGGER_MS="${STAGGER_MS:--1}"       # -1 => alternate lockstep/jitter
WAIT_TIMEOUT="${WAIT_TIMEOUT:-20m}"
BLOB_THRESHOLD="${BLOB_THRESHOLD:-}" # e.g. 128MiB; empty => regular cold GET
KEEP_MD="${KEEP_MD:-1}"              # evict --keep-md by default
LOG_DIR="${LOG_DIR:-}"               # opt-in only; avoids stale log matches
VERBOSE=0
KEEP_WORKDIR=0

usage() {
  awk '
    NR == 1 { next }             # skip
    /^#/ {
      sub(/^# ?/, "")
      print
      seen = 1
      next
    }
    seen { exit }                # stop after the leading comment block
  ' "$0" >&2
}

die() {
  echo "FATAL: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
}

# ----- args -----

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)         BCK="$2"; shift 2 ;;
    --prefix)         PREFIX="$2"; shift 2 ;;
    --rounds)         ROUNDS="$2"; shift 2 ;;
    --par)            PAR="$2"; shift 2 ;;
    --workers)        WORKERS="$2"; shift 2 ;;
    --stagger-ms)     STAGGER_MS="$2"; shift 2 ;;
    --timeout)        WAIT_TIMEOUT="$2"; shift 2 ;;
    --blob-threshold) BLOB_THRESHOLD="$2"; shift 2 ;;
    --no-keep-md)     KEEP_MD=0; shift ;;
    --log-dir)        LOG_DIR="$2"; shift 2 ;;
    --keep-workdir)   KEEP_WORKDIR=1; shift ;;
    -v|--verbose)     VERBOSE=1; shift ;;
    -h|--help)        usage; exit 0 ;;
    *)                usage; die "unknown arg: $1" ;;
  esac
done

[[ -n "$BCK" ]] || { usage; die "--bucket is required"; }
command -v ais >/dev/null 2>&1 || die "'ais' CLI not found in PATH"

# ----- temp/capture dir -----

WORKDIR="$(mktemp -d -t prefetch-collisions.XXXXXX)"
if [[ "$KEEP_WORKDIR" -eq 0 ]]; then
  trap 'rm -rf "$WORKDIR"' EXIT
else
  echo "workdir: $WORKDIR"
fi

# selector as an array: avoids quoting/word-splitting issues

SEL=()
[[ -n "$PREFIX" ]] && SEL=(--prefix "$PREFIX")

tail_file() {
  local file="$1"
  local lines="${2:-80}"
  [[ -s "$file" ]] || return 0
  echo "----- $(basename "$file") last $lines lines -----" >&2
  tail -n "$lines" "$file" >&2 || true
}

run_capture() {
  local outfile="$1"; shift
  "$@" >"$outfile" 2>&1
}

# ----- preflight -----

PRE="$WORKDIR/preflight.bucket-props.out"
run_capture "$PRE" ais bucket props "$BCK" || {
  tail_file "$PRE" 80
  die "cannot access bucket $BCK"
}

dashboard_health() {  # -> "state<TAB>targets<TAB>online<TAB>ioerrors"
  ais show dashboard 2>/dev/null | awk -F: '
    {
      key = $1
      val = $2
      sub(/^[[:space:]]+/, "", key)
      sub(/[[:space:]]+$/, "", key)
      sub(/^[[:space:]]+/, "", val)
    }
    key == "State" && state == "" {
      split(val, a, /[[:space:]]+/)
      state = a[1]
    }
    key == "I/O Errors" {
      split(val, a, /[[:space:]]+/)
      ioerr = a[1]
    }
    key == "Targets" {
      split(val, a, /[[:space:]]+/)
      tgts = a[1]
    }
    key == "Status" {
      split(val, a, /[[:space:]]+/)
      online = a[1]
    }
    END { printf "%s\t%s\t%s\t%s\n", state, tgts, online, ioerr }
  '
}

IFS=$'\t' read -r STATE0 TGT0 ONL0 IOERR0 < <(dashboard_health)

[[ "${TGT0:-}" =~ ^[0-9]+$ && "$TGT0" -gt 0 ]] || die "dashboard reports no targets"
[[ "${ONL0:-}" =~ ^[0-9]+$ && "$ONL0" -ge "$TGT0" ]] || die "dashboard did not report sane online status"

echo "cluster: State=${STATE0:-unknown}, targets=$TGT0, online=$ONL0, ioerr=${IOERR0:-?}"

echo "bucket=$BCK prefix=${PREFIX:-<none>} rounds=$ROUNDS par=$PAR workers=$WORKERS" \
  "stagger-ms=$STAGGER_MS blob=${BLOB_THRESHOLD:-off} keep-md=$KEEP_MD"

if [[ "$VERBOSE" -eq 1 ]]; then
  SUMMARY="$WORKDIR/preflight.summary.out"
  run_capture "$SUMMARY" ais ls "$BCK" "${SEL[@]}" --summary --all || true
  tail_file "$SUMMARY" 20
fi

cluster_ok() {
  local state tgts online ioerr
  IFS=$'\t' read -r state tgts online ioerr < <(dashboard_health)

  if [[ "$state" != "Operational" ]]; then
    warn "cluster State=$state, expected Operational" >&2
  fi
  if [[ "$tgts" != "$TGT0" || "$online" != "$ONL0" ]]; then
    echo "FATAL: membership changed: targets $TGT0->$tgts, online $ONL0->$online" >&2
    return 1
  fi
  if [[ -n "${ioerr:-}" && "$ioerr" =~ ^[0-9]+$ &&
        -n "${IOERR0:-}" && "${IOERR0:-0}" =~ ^[0-9]+$ &&
        "$ioerr" -gt "$IOERR0" ]]; then
    warn "I/O errors ${IOERR0:-0} -> $ioerr; check target logs"
  fi
  return 0
}

RACE_RE='DATA RACE|concurrent map (writes|read and write)|fatal error: concurrent|panic: '

scan_logs() {
  [[ -n "$LOG_DIR" ]] || return 0
  [[ -d "$LOG_DIR" ]] || { warn "log dir does not exist: $LOG_DIR"; return 0; }

  local hit
  hit="$(grep -rEl "$RACE_RE" "$LOG_DIR" 2>/dev/null | head -n 1 || true)"
  if [[ -n "$hit" ]]; then
    echo "FATAL: race/panic signature found under $LOG_DIR" >&2
    grep -rEn "$RACE_RE" "$LOG_DIR" 2>/dev/null | head -20 >&2 || true
    return 1
  fi
  return 0
}

dump_diag() {
  echo "----- diagnostics -----" >&2

  ais show dashboard 2>&1 | sed -n '1,45p' >&2 || true
  echo >&2

  ais show job evict-listrange 2>&1 | tail -40 >&2 || true
  echo >&2

  ais show job prefetch-listrange 2>&1 | tail -80 >&2 || true
}

prefetch_one() {
  local args=(start prefetch "$BCK")

  [[ -n "$PREFIX" ]] && args+=(--prefix "$PREFIX")

  args+=(--latest --num-workers "$WORKERS" --wait)
  [[ -n "$WAIT_TIMEOUT" ]] && args+=(--timeout "$WAIT_TIMEOUT")
  [[ -n "$BLOB_THRESHOLD" ]] && args+=(--blob-threshold "$BLOB_THRESHOLD")

  ais "${args[@]}"
}

evict_set() {
  local outfile="$1"
  local args=(bucket evict "$BCK")

  [[ -n "$PREFIX" ]] && args+=(--prefix "$PREFIX")
  [[ "$KEEP_MD" -eq 1 ]] && args+=(--keep-md)

  args+=(--yes --wait --nv)
  [[ -n "$WAIT_TIMEOUT" ]] && args+=(--timeout "$WAIT_TIMEOUT")

  run_capture "$outfile" ais "${args[@]}"
}

round_stagger_ms() {
  local round="$1"

  if [[ "$STAGGER_MS" -ge 0 ]]; then
    echo "$STAGGER_MS"
    return
  fi

  # Odd rounds: lockstep for maximum same-name collision.
  # Even rounds: jitter for wider timing-window coverage.

  if (( round % 2 == 1 )); then
    echo 0
  else
    echo 400
  fi
}

sleep_ms_jitter() {
  local max_ms="$1"
  (( max_ms > 0 )) || return 0

  local ms
  ms=$(( (RANDOM % max_ms) + 1 ))
  sleep "$(printf '%d.%03d' $((ms / 1000)) $((ms % 1000)))"
}

# ----- main -----

for r in $(seq 1 "$ROUNDS"); do
  echo "round $r/$ROUNDS: evict"

  evict_out="$WORKDIR/round-${r}-evict.out"
  if ! evict_set "$evict_out"; then
    tail_file "$evict_out" 120
    dump_diag
    die "round $r: evict failed"
  fi

  if [[ "$VERBOSE" -eq 1 ]]; then
    tail_file "$evict_out" 20
  fi

  stagger="$(round_stagger_ms "$r")"
  echo "round $r/$ROUNDS: prefetch par=$PAR workers=$WORKERS stagger=${stagger}ms"

  pids=()
  files=()

  for j in $(seq 1 "$PAR"); do
    out="$WORKDIR/round-${r}-prefetch-${j}.out"
    files+=("$out")

    (
      prefetch_one
    ) >"$out" 2>&1 &

    pids+=("$!")
    sleep_ms_jitter "$stagger"
  done

  fail=0
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
      fail=1
      echo "FATAL: round $r prefetch job $((i + 1)) failed" >&2
      tail_file "${files[$i]}" 160
    elif [[ "$VERBOSE" -eq 1 ]]; then
      tail_file "${files[$i]}" 30
    fi
  done

  if (( fail != 0 )); then
    dump_diag
    scan_logs || true
    exit 1
  fi

  if ! cluster_ok; then
    dump_diag
    scan_logs || true
    exit 1
  fi

  if ! scan_logs; then
    dump_diag
    exit 1
  fi
done

echo "completed $ROUNDS rounds, $PAR-way concurrency, no race/panic detected"
