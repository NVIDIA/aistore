sudo cp dfc.json dfcproxy.json
sed -i '/logdir/c\"logdir": "/var/log/dfcproxy",' dfcproxy.json
export DFCPRIMARYPROXY=True
sudo -E /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfcproxy.json -role=proxy -ntargets=6 &

