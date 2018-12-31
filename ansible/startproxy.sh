#!/bin/bash
set -e
sudo cp dfc.json dfcproxy.json
sed -i '/logdir/c\"logdir": "/var/log/dfcproxy",' dfcproxy.json
sed -i '/"port":/c\"port": "8082",' dfcproxy.json
sudo /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfcproxy.json -role=proxy -ntargets=6 &

