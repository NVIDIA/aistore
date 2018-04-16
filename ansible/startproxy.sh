ps -C dfc -o pid= | xargs sudo kill -9
cp dfc.json dfcproxy.json
sed -i '/logdir/c\"logdir": "/var/log/dfcproxy"' dfcproxy.json
sudo /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfcproxy.json -role=proxy -ntargets=6 &

