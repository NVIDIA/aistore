#!/bin/bash
set -e
cp dfc.json dfcproxy.json
sed -i '/logdir/c\"logdir": "/var/log/dfcproxy",' dfcproxy.json
sed -i '/"port":/c\"port": "8082"' dfcproxy.json
sudo /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfcproxy.json -role=proxy -ntargets=6 &
if ! ps -C dfc -o pid= ; then
	echo stub proxy started on host `hostname`
else
	echo failed to start stub proxy on host `hostname`
fi

