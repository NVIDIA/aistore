#!/bin/bash

usage() {
    echo
    echo "Usage: $0 kube_conf app_name docker_repo_token";
    echo " where :"
    echo "  kube_conf         : path to kube-config to the K8S cluster"
    echo "  app_name          : helm application name to deploy"
    echo "  docker_repo_token : auth token to pull the docker image to deploy the helm chart"
    echo
    exit -1
}

if [ $# != 3 ]; then
    usage
fi

export KUBECONFIG=$1
APP_NAME=$2
DOCKER_REPO_TOKEN=$3

# TODO: ADD CHECKS TO ENSURE THE TWO REQUIRED PARAMETERS ARE SPECIFIED.

set -e

helm_args=""

echo "Check if there is an image_tag.txt file override"

if [[ -f image_tag.txt ]]; then
    image_tag=`cat image_tag.txt`
    helm_args=" --set image.tag=${image_tag} "
    echo "helm install with ARGS: ${helm_args}"
fi

if [[ ! -z $DOCKER_REPO_TOKEN ]];then
  helm_args+=" -- set image.dockerRepoToken=\"$DOCKER_REPO_TOKEN\""
else
  echo "ERROR: DOCKER_REPO_TOKEN needs to be passed" >&2
  usage;
fi

# Check if the app has been installed
if helm list | grep ${APP_NAME} ; then

    # Still need to use delete as the deploy.strategy of 'Recreate' doesn't seem to work
    #echo "${APP_NAME} has been installed. Upgrading..."
    #helm upgrade ${APP_NAME} .

    echo "${APP_NAME} has been installed. Deleting..."
    helm delete --purge ${APP_NAME}
fi
#else
#echo "${APP_NAME} does not exist. Installing..."

    echo "Installing ${APP_NAME}..."
    helm dependency update
    helm install --name=${APP_NAME} ${helm_args}  .
#fi

