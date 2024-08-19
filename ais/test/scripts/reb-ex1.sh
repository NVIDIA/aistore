#!/bin/bash
for i in {1..6}; do
  ais cluster add-remove-nodes shutdown t[Qxut8086] --no-rebalance --yes
  sleep 3
  aisnode -config=/root/.ais6/ais.json -local_config=/root/.ais6/ais_local.json -role=target &
  sleep 3
  ais cluster add-remove-nodes stop-maintenance t[Qxut8086] --yes
  sleep 3
  ais cluster add-remove-nodes shutdown t[RVst8090] --no-rebalance --yes
  sleep 3
  aisnode -config=/root/.ais10/ais.json -local_config=/root/.ais10/ais_local.json -role=target &
  sleep 3
  ais cluster add-remove-nodes stop-maintenance t[RVst8090] --yes
  sleep 3
done
