echo "Creating GCP credentials secret..."
kubectl create secret generic gcp-credentials \
    --from-file=creds.json=${GOOGLE_APPLICATION_CREDENTIALS}

echo "Creating AWS credentials secrets..."
kubectl create secret generic aws-credentials \
    --from-literal=AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    --from-literal=AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --from-literal=AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}

echo "Creating OCI credentials secret..."
kubectl create secret generic oci-credentials \
    --from-file=OCI_PRIVATE_KEY=${ORACLE_PRIVATE_KEY} \
    --from-literal=OCI_TENANCY_OCID=${OCI_TENANCY_OCID} \
    --from-literal=OCI_USER_OCID=${OCI_USER_OCID} \
    --from-literal=OCI_REGION=${OCI_REGION} \
    --from-literal=OCI_FINGERPRINT=${OCI_FINGERPRINT} \
    --from-literal=OCI_COMPARTMENT_OCID=${OCI_COMPARTMENT_OCID}