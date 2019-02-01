sudo cp ais.json aisproxy.json
sed -i '/logdir/c\"logdir": "/var/log/aisproxy",' aisproxy.json
export AIS_PRIMARYPROXY=True
sudo -E /home/ubuntu/ais/bin/ais -config=/home/ubuntu/aisproxy.json -role=proxy -ntargets=6 &

