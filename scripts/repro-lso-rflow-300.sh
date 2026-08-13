#!/bin/bash
set -euo pipefail

# Requires a clean local playground; run on an unfixed revision to reproduce.
cd "$(dirname "$0")/.."
gopath="$(go env GOPATH)"
export PATH="$gopath/bin:$PATH"
primary=http://127.0.0.1:20080
remote=http://127.0.0.1:23080

if pgrep -x aisnode >/dev/null || compgen -G "$HOME/.ais[0-9]*" >/dev/null ||
  compgen -G "$HOME/.ais_next*" >/dev/null || [[ -e /tmp/ais || -e /tmp/ais_next ]]; then
  echo "local AIS state exists; archive it or run 'make kill clean' first" >&2
  exit 2
fi
make cli
out=$(mktemp -d /tmp/lso-rflow-deadlock.XXXXXX)

# 1 proxy + 300 targets; high port ranges avoid common local port collisions.
printf '300\n1\n1\nn\nn\nn\nn\n\nn\n' | PORT=20080 PORT_INTRA_CONTROL=21080 PORT_INTRA_DATA=22080 make deploy >"$out/primary-deploy.log" 2>&1
printf '1\n1\n1\nn\nn\nn\nn\n\nn\n' | DEPLOY_AS_NEXT_TIER=1 PORT=23080 PORT_INTRA_CONTROL=24080 PORT_INTRA_DATA=25080 make deploy >"$out/remote-deploy.log" 2>&1

wait_cluster() {
  for _ in {1..180}; do
    AIS_ENDPOINT="$1" ais show cluster 2>/dev/null | grep -q "$2 online" && return
    sleep 1
  done
  return 1
}
wait_cluster "$primary" 301
wait_cluster "$remote" 2

AIS_ENDPOINT="$remote" ais bucket create ais://lso-deadlock
printf x | AIS_ENDPOINT="$remote" ais put - ais://lso-deadlock/one
AIS_ENDPOINT="$primary" ais cluster remote-attach "remais=$remote"
AIS_ENDPOINT="$primary" ais ls ais://@remais/lso-deadlock --all --props=name >/dev/null

deadlocks=0 failures=0
for i in {1..10}; do
  if timeout 75s env AIS_ENDPOINT="$primary" ais ls ais://@remais/lso-deadlock --all --props=name,location >"$out/$i.out" 2>"$out/$i.err"; then
    echo "$i PASS"
  elif [[ $? == 124 ]] || grep -q 'context deadline exceeded' "$out/$i.err"; then
    deadlocks=$((deadlocks + 1)); echo "$i DEADLOCK"
  else
    failures=$((failures + 1))
    echo "$i OTHER-FAIL: $(<"$out/$i.err")"
  fi
done

echo "deadlocks=$deadlocks/10 failures=$failures/10 logs=$out"
echo "--- proxy commit timeouts ---"
grep 'failed to call.*commit' /tmp/ais/0/log/aisproxy.ERROR | tail -10 || true
echo "--- targets receiving commit after LIST expiry ---"
grep -h 'late commit phase' /tmp/ais/{1..300}/log/aistarget.INFO | tail -10 || true
if ((deadlocks != 0 || failures != 0)); then
  exit 1
fi
