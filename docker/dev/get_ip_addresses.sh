#!/bin/bash
# The following script lists the public, internal and replication network IP Addresses for
# each daemon.
setup_file="/tmp/docker_dfc/deploy.env"
if [ -f $"$setup_file" ]; then
    # get $PORT, $PORT_INTRA_CONTROL and $PORT_INTRA_DATA
    source $setup_file
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
                if [[ -z "${PORT}" ]]; then
                    echo "    $network: $ip_address:8080"
                else
                    echo "    $network: $ip_address:$PORT"
                fi
            elif [[ "$network" == *"internal_control"* ]]; then
                if [[ -z "${PORT_INTRA_CONTROL}" ]]; then
                    echo "    $network: $ip_address:9080"
                else
                    echo "    $network: $ip_address:$PORT_INTRA_CONTROL"
                fi
            else 
                if [[ -z "${PORT_INTRA_DATA}" ]]; then
                    echo "    $network: $ip_address:10080"
                else
                    echo "    $network: $ip_address:$PORT_INTRA_DATA"
                fi
            fi
        
        fi
    done
    echo
done
echo "Note if the docker deploy script was called from a different terminal window, the correct port numbers might not be listed above."

