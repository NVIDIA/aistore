#!/bin/bash

docker --version

if [ $? -ne 0 ]; then
  echo "Docker not installed or running properly."
  exit 1
fi

minikube version

if [ $? -ne 0 ]; then
  echo "Minikube not installed or running properly."
  exit 1
fi
