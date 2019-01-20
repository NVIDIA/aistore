#!/bin/bash
set -e
sudo cp ais.json aisproxy.json
sed -i '/logdir/c\"logdir": "/var/log/aisproxy",' aisproxy.json
sed -i '/"port":/c\"port": "8082",' aisproxy.json
sudo /home/ubuntu/ais/bin/ais -config=/home/ubuntu/aisproxy.json -role=proxy -ntargets=6 &

