#!/bin/bash

# install cert-manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.1/cert-manager.yaml

sleep 10

# Namespace where cert-manager is installed
cert_manager_namespace="cert-manager"

# Get the list of pod names in the cert-manager namespace
pod_list=$(kubectl get pods -n "$cert_manager_namespace" -o custom-columns=:metadata.name --no-headers)

# Check if there are any pods in the list
if [ -z "$pod_list" ]; then
    echo "No pods found in namespace $cert_manager_namespace."
    exit 1
fi

# Print the list of pod names
echo "List of pods in namespace $cert_manager_namespace:"
echo "$pod_list"

# Function to check if a Kubernetes pod is in the "Running" state
function is_pod_running {
    local pod_name="$1"
    local pod_status=$(kubectl get pod -n "$cert_manager_namespace" "$pod_name" -o jsonpath='{.status.phase}')
    
    if [ "$pod_status" == "Running" ]; then
        return 0  # Pod is running
    else
        return 1  # Pod is not running
    fi
}

# Function to check if a Kubernetes pod is ready
function is_pod_ready {
    local pod_name="$1"
    local pod_condition=$(kubectl get pod -n "$cert_manager_namespace" "$pod_name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
    
    if [ "$pod_condition" == "True" ]; then
        return 0  # Pod is ready
    else
        return 1  # Pod is not ready
    fi
}

# Maximum timeout (in seconds)
timeout_seconds=30
start_time=$(date +%s)

# Wait for all cert-manager pods to be in the "Running" and "Ready" state with a timeout
for pod_name in $pod_list; do
    while ! is_pod_running "$pod_name" || ! is_pod_ready "$pod_name"; do
        current_time=$(date +%s)
        elapsed_time=$((current_time - start_time))

        if [ "$elapsed_time" -ge "$timeout_seconds" ]; then
            echo "Timeout of $timeout_seconds seconds reached. Exiting."
            exit 1
        fi

        echo "Waiting for pod $pod_name in namespace $cert_manager_namespace to be in the Running/Ready state..."
        sleep 5  # Adjust the sleep interval as needed
    done
    echo "Pod $pod_name in namespace $cert_manager_namespace is Running/Ready state."
done

# Proceed with the next operations after all pods are ready
echo "All cert-manager pods in namespace $cert_manager_namespace are Running and Ready. Proceeding with next operations..."
# Your next operations here

# create cert-issuer and certificate
kubectl apply -f kube_templates/cert_manager.yaml