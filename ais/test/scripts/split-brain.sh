#!/bin/bash

# Runtime defaults
sleep_time=5
iterations=3

if ! [ -z "$1" ]; then
  iterations=$1
fi

# Always the case
orig_primary_endpoint=http://localhost:8080
proxy_that_always_gets_splintered=http://localhost:8085 ## ais/usr1deb impl.

# Temp files
show_cluster_out_1="/tmp/cluster_output_1_${PID}.txt"
show_cluster_out_2="/tmp/cluster_output_2_${PID}.txt"
show_smap_out_2="/tmp/smap_output_2_${PID}.txt"

# Cleanup
cleanup() {
  rm -f "$show_cluster_out_1" "$show_cluster_out_2" "$show_smap_out_2" 2>/dev/null
}

trap cleanup EXIT

# Remember original counts, proxies and targets respectively
ais show cluster > $show_cluster_out_1 || exit $?
orig_proxies=$(grep -c "^p\[" $show_cluster_out_1)
orig_targets=$(grep -c "^t\[" $show_cluster_out_1)

# set -x ## uncomment to debug

for i in $(seq 1 1 $iterations); do
  echo "Iteration #$i --------------------------------------------------------------------------------------------"

  # STEP 1: Trigger split-brain
  echo "STEP 1: Triggering split-brain..."
  pgrep -a aisnode | grep -v "ais_next" | awk '{print $1}' | xargs -I {} kill -USR1 {}

  # Sleep for a few seconds to allow the split-brain to take effect
  sleep $sleep_time

  # STEP 2: Parse primary IDs and endpoints
  echo "STEP 2: Parse primary IDs and endpoints..."
  ais show cluster > $show_cluster_out_1 || exit $?

  orig_pid=$(grep -oP 'p\[\K[^\]]+(?=\]\[P\])' $show_cluster_out_1)

  [[ ! -z "$orig_primary_endpoint" ]] || { echo "Error: no original primary ID"; exit 1; }

  # Parse splintered_primary_endpoint
  AIS_ENDPOINT="$proxy_that_always_gets_splintered" ais show cluster > $show_cluster_out_2
  AIS_ENDPOINT=http://localhost:8085 ais show cluster smap > $show_smap_out_2

  splintered_primary_id=$(grep -oP 'p\[\K[^\]]+(?=\]\[P\])' $show_cluster_out_2)
  splintered_primary_endpoint=$(awk -v id="$splintered_primary_id" '$1 ~ id {print $3}' $show_smap_out_2)

  [[ ! -z "$splintered_primary_endpoint" ]] || { echo "Error: no second primary endpoint"; exit 1; }

  # STEP 3: Disable global rebalance (no rebalancing when merging clusters)
  echo "STEP 3: Disabling global rebalance (no rebalancing when merging clusters)..."
  ais config cluster rebalance.enabled false || exit $?

  # STEP 4: Resolve split-brain
  echo "STEP 4: Resolving split-brain..."
  AIS_ENDPOINT=$splintered_primary_endpoint ais cluster set-primary $orig_pid $orig_primary_endpoint --force

  # Sleep for a few seconds to allow the cluster to merge
  sleep $sleep_time

  # STEP 5: Observe unified cluster and validate results
  echo "STEP 5: Observing unified cluster and validating results..."
  ais show cluster > $show_cluster_out_1 || exit $?

  current_proxies=$(grep -c "^p\[" $show_cluster_out_1)
  [[ "$current_proxies" -eq "$orig_proxies" ]] || { echo "Error: wrong number of _unified_ proxies: expected $orig_proxies, got $current_proxies"; exit 1; }

  current_targets=$(grep -c "^t\[" $show_cluster_out_1)
  [[ "$current_targets" -eq "$orig_targets" ]] || { echo "Error: wrong number of _unified_ targets: expected $orig_targets, got $current_targets"; exit 1; }

  # STEP 6: Re-enable global rebalance
  echo "STEP 6: Re-enabling global rebalance..."
  ais config cluster rebalance.enabled true || exit $?

  # Wait for the next iteration
  if [ "$i" -lt "$iterations" ]; then
    echo "Waiting for the next iteration..."
    sleep $sleep_time
    echo
  fi
done

echo
echo "Done."
