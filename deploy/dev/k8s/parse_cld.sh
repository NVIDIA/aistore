
export LOCAL_AWS="/tmp/aws.env"
export AIS_CLD_PROVIDER="" # See deploy.sh for more informations about empty AIS_CLD_PROVIDER
echo "Select:"
echo " 0: No 3rd party Cloud"
echo " 1: Amazon S3"
echo " 2: Google Cloud Storage"
echo " 3: Azure Cloud"
read cldprovider
if [ $cldprovider -eq 1 ]; then
    export AIS_CLD_PROVIDER="aws"

    echo "Enter the location of your AWS configuration and credentials files:"
    echo "Note: No input will result in using the default AWS dir (~/.aws/)"
    read aws_env

    if [ -z "$aws_env" ]; then
        aws_env="~/.aws/"
    fi
    # to get proper tilde expansion
    aws_env="${aws_env/#\~/$HOME}"
    temp_file="$aws_env/credentials"
    if [ -f $"$temp_file" ]; then
        cp $"$temp_file"  ${LOCAL_AWS}
    else
        echo "No AWS credentials file found in specified directory. Exiting..."
        exit 1
    fi

    # By default, the region field is found in the aws config file.
    # Sometimes it is found in the credentials file.
    if [ $(cat "$temp_file" | grep -c "region") -eq 0 ]; then
        temp_file="$aws_env/config"
        if [ -f $"$temp_file" ] && [ $(cat $"$temp_file" | grep -c "region") -gt 0 ]; then
            grep region "$temp_file" >> ${LOCAL_AWS}
        else
            echo "No region config field found in aws directory. Exiting..."
            exit 1
        fi
    fi

    sed -i 's/\[default\]//g' ${LOCAL_AWS}
    sed -i 's/ = /=/g' ${LOCAL_AWS}
    sed -i 's/aws_access_key_id/AWS_ACCESS_KEY_ID/g' ${LOCAL_AWS}
    sed -i 's/aws_secret_access_key/AWS_SECRET_ACCESS_KEY/g' ${LOCAL_AWS}
    sed -i 's/region/AWS_DEFAULT_REGION/g' ${LOCAL_AWS}

elif [[ $cldprovider -eq 2 ]]; then
  export AIS_CLD_PROVIDER="gcp"
elif [[ $cldprovider -eq 3 ]]; then
  export AIS_CLD_PROVIDER="azure"
else
  export AIS_CLD_PROVIDER=""
fi

if kubectl get secrets | grep aws > /dev/null 2>&1; then
    kubectl delete secret aws-credentials
fi
kubectl create secret generic aws-credentials --from-file=$LOCAL_AWS
