#!/bin/bash
# The following script is used to open an interactive shell for a specified running daemon. 
# It currently defaults to open the /tmp/dfc/log working directory.
if [ $# -eq 1 ] && [[ $1 == "dfc"* ]] ; then
    echo "Opening an interactive bash shell for container" $1
else
    echo "Incorrect usage, example usage:       ./container_logs.sh CONTAINER_NAME"
    echo "To view all containters, execute:     docker ps"
    exit 1  
fi

valid_container_name() {
    found=FALSE
    for container_name in $(docker ps --format "{{.Names}}"); do
        if [ "$1" == "$container_name" ]; then
            found=TRUE
            break
        fi
    done

    if [ "$found" == "FALSE" ]; then
        echo "Not a valid container name."
        exit 1
    fi
}

valid_container_name $1
docker exec -t -w /tmp/dfc/log -i $1 /bin/bash
