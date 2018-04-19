#!/bin/bash
set -e
sudo /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfc.json -role=target &
if ! ps -C dfc -o pid= ; then
	echo target started on host `hostname`
else
	echo failed to start target on host `hostname`
fi
