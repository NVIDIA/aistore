#!/bin/bash

# Remove the old version
FILE=${GOPATH}/bin/ais
if [[ -x "$FILE" ]]; then
    rm ${FILE}
fi

VERSION="0.3"
BUILD=`git rev-parse --short HEAD`
BINARY_NAME="ais"
URL="http://127.0.0.1:8080"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_SCRIPT_DIR="${DIR}/autocomplete"
AUTOCOMPLETE_INSTALL_SCRIPT="${AUTOCOMPLETE_SCRIPT_DIR}/install.sh"

getDockerURL() {
    proxy_name="ais0_proxy_1"
    container_id=$(docker ps -qf name=${proxy_name})

    if [[ "$container_id" != "" ]]; then
        docker_ip_list=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' ${container_id})
        docker_ip=$(echo ${docker_ip_list} |  awk '{print $1;}')
    else 
        echo "Could not get Docker container ID"
        exit 1
    fi

    port_list=$(docker inspect --format='{{range $p, $conf := .NetworkSettings.Ports}}{{$p}} {{end}}' ${container_id})
    for port in ${port_list}; do
        if [[ ${port} == 8* ]]; then
            docker_port=$(echo ${port} | awk -F/ '{print $1}')
            break
        fi
    done

    URL="http://${docker_ip}:${docker_port}"
}

# If docker is running use docker's URL
hash docker &>/dev/null
if [[ "$?" == "0" ]]; then
    docker_running=$(docker container ls)
    if [[ "$?" != "0" ]]; then
        echo "Warning: Can't check if AIS is running from docker, verify that you have permissions for /var/run/docker.sock" >&2
    elif [[ "$(echo ${docker_running} | grep ${BINARY_NAME})" != "" ]]; then
        getDockerURL
    fi
fi

# Global env AIS_URL overrides all
if [[ ! -z "${AIS_URL}" ]]; then
    URL=${AIS_URL}
else 
    echo "AIS_URL env variable is not set. CLI will use ${URL} as default URL. To make it use a different one, run export AIS_URL=<URL>."
fi

# Install the CLI
GOBIN=${GOPATH}/bin go install -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}' -X 'main.url=${URL}'" ${DIR}/${BINARY_NAME}.go

if [[ "$?" -eq 0 ]]; then
    echo "*** AIS CLI successfully installed."
    echo "*** "

    # Install autocompletions
    bash ${AUTOCOMPLETE_INSTALL_SCRIPT}
else
    echo "Error installing AIS CLI."
fi
