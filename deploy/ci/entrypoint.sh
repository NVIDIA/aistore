#!/bin/bash
set -e

sysctl -w net.ipv6.conf.all.disable_ipv6=0
sysctl -w net.ipv6.conf.default.disable_ipv6=0

# For AIStore memory management and pressure detection (see docs/performance.md)
sysctl -w vm.swappiness=10

exec "$@"
