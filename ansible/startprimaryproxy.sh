ps -C dfc -o pid= | xargs sudo kill -9
export DFCPRIMARYPROXY=True
sudo -E /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfc.json -role=proxy -ntargets=6 &

