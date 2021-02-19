cat ais.json | jq '.log.dir |= "/var/log/aisproxy"' > aisproxy.json
export AIS_IS_PRIMARY=True
sudo -E /home/ubuntu/ais/bin/aisnode -config=/home/ubuntu/aisproxy.json -local_config=/home/ubuntu/ais_local.json -role=proxy -ntargets=6 &

