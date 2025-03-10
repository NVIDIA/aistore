#!/bin/bash

kubectl delete secret gcp-credentials || true
kubectl delete secret aws-credentials || true
kubectl delete secret oci-credentials || true
