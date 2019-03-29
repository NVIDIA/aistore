#!/bin/bash
set -e
sudo cp ais.json aisproxy.json

cat aisproxy.json | jq '.log.dir = "/var/log/aisproxy" | .net.l4.port = "8082"' > aisproxy.json
sudo /home/ubuntu/ais/bin/aisnode -config=/home/ubuntu/aisproxy.json -role=proxy -ntargets=6 &

