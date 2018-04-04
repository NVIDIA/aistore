ps -C dfc -o pid= | xargs sudo kill -9
sudo $GOBIN/dfc -config=/home/ubuntu/dfc.json -role=proxy -ntargets=6 &

