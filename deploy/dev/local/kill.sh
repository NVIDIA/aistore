#!/bin/bash

############################################
#
# Usage: kill.sh [--force|-f]
#
############################################

wait_for() {
  i=0
  for pid in $(pgrep "$1" | tr '\n' ' '); do
    while kill -0 "${pid}" 2>/dev/null; do
      sleep 0.1
      i=$((i + 1))
      if [[ $i -ge 70 ]]; then # timeout 7s
         break 2
      fi
    done
  done
  for pid in $(pgrep "$1" | tr '\n' ' '); do
    kill -9 "${pid}" 2>/dev/null
  done
}


signal=SIGINT

while (( "$#" )); do
  case "$1" in
    --force|-f) signal=SIGKILL; shift;;
    *) echo "fatal: unknown argument '$1'"; exit 1;;
  esac
done

pkill "-${signal}" aisnode
wait_for aisnode

pkill "-${signal}" authn
wait_for authn
