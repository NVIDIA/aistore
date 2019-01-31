#!/bin/bash

# The following script lists the public, internal and replication network IP
# Addresses for each daemon.
SETUP_FILE="/tmp/docker_ais/deploy.env"
if [ -f $"$SETUP_FILE" ]; then
    # get $PORT, $PORT_INTRA_CONTROL and $PORT_INTRA_DATA
    source $SETUP_FILE
fi

for container_name in $(docker ps --format "{{.Names}}"); do
    connected_networks=$(docker inspect -f '{{range $key, $value := .NetworkSettings.Networks}}{{printf "%s " $key}}{{end}}' $container_name)
    echo $container_name
    for network in $connected_networks
    do
        if [[ $network =~ ^${container_name:0:5} ]]
        then
            format="'{{.NetworkSettings.Networks.${network}.IPAddress}}'"
            ip_address=$(docker inspect -f $format $container_name)
            ip_address=${ip_address//\'/}   # remove all single quotes
            if [[ "$network" == *"public"* ]]; then
                echo "    $network: $ip_address:${PORT:-8080}"
            elif [[ "$network" == *"internal_control"* ]]; then
                echo "    $network: $ip_address:${PORT_INTRA_CONTROL:-9080}"
            else 
                echo "    $network: $ip_address:${PORT_INTRA_DATA:-10080}"
            fi
        fi
    done
    echo
done

echo "Note if the docker deploy script was called from a different terminal window, the correct port numbers might not be listed above."

