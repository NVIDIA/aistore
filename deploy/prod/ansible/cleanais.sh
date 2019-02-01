#!/bin/bash
set -o
./stopandcleanais.sh
echo "Cleaning up targets"
parallel-ssh -h inventory/targets.txt -i './cleanaisstate.sh'
if [[ -s inventory/new_targets.txt ]]; then parallel-ssh -h inventory/new_targets.txt -i './cleanaisstate.sh'; fi
echo "Cleaning up proxy"
parallel-ssh -h inventory/proxy.txt -i './cleanaisstate.sh'
