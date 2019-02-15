#!/bin/bash


export KUBECONFIG=$1
APP_NAME=$2

//TODO: ADD CHECKS TO ENSURE THE TWO REQUIRED PARAMETERS ARE SPECIFIED

set -e

helm_args=""

echo "Check if there is an image_tag.txt file override"

if [[ -f image_tag.txt ]]; then
    image_tag=`cat image_tag.txt`
    helm_args=" --set image.tag=${image_tag} "
    echo "helm install with ARGS: ${helm_args}"
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
    helm install --name=${APP_NAME} ${helm_args}  .
#fi


echo "Waiting ${APP_NAME} to come up..."

kubectl rollout status ds/${APP_NAME}-proxy
kubectl rollout status ds/${APP_NAME}-target
