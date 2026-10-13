#!/bin/bash
#
# Check that this host or container can run AIS targets with kTLS transmit
# offload (see docs/https.md, "kTLS transmit offload").
#
# The check needs /proc/net/tls_stat, which has the kTLS counters. The file
# exists only when the `tls` kernel module is loaded, and only on Linux 5.5 or
# later. Before Linux 5.5, kTLS can work, but this script cannot show it.
#
# Example usage:
# ./check-ktls.sh          # before the run: can this host use kTLS?
# ./check-ktls.sh --armed  # after the run: did any connection arm kTLS?
#
# The counters are per network namespace. Run --armed in the same host or
# container as the targets.

set -euo pipefail

if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--armed" ) ]]; then
	echo "Usage: ${0##*/} [--armed]" >&2
	exit 2
fi

if [[ "$(uname -s)" != "Linux" ]]; then
	echo "ERROR: kTLS needs Linux (found $(uname -s))" >&2
	exit 1
fi
if [[ ! -e /proc/net/tls_stat ]]; then
	if [[ -d /sys/module/tls ]]; then
		echo "ERROR: no /proc/net/tls_stat, needs Linux 5.5 or later (found $(uname -r))" >&2
	else
		echo "ERROR: the tls kernel module is not loaded (run 'sudo modprobe tls' on the host)" >&2
	fi
	exit 1
fi
echo "kTLS: available (Linux $(uname -r))"

if [[ "${1:-}" == "--armed" ]]; then
	awk '/^TlsTx(Sw|Device)/ {print; n += $2} END {fflush(); if (n == 0) print "ERROR: no connection armed kTLS" > "/dev/stderr"; exit n == 0}' /proc/net/tls_stat
fi
