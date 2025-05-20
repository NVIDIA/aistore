#!/bin/bash

echo "Creating Docker Hub credentials secret..."
kubectl create secret docker-registry regcred \
    --docker-username=${DOCKERHUB_USERNAME} \
    --docker-password=${DOCKERHUB_TOKEN}