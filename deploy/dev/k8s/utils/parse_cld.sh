
export LOCAL_AWS="/tmp/credentials"
touch $LOCAL_AWS
export AIS_BACKEND_PROVIDERS="" # See deploy.sh for more information about empty AIS_BACKEND_PROVIDERS.
source ../utils.sh
set_env_backends

if [[ "${AIS_BACKEND_PROVIDERS}" == *aws* ]]; then
    echo "Enter the location of your AWS configuration and credentials files:"
    echo "Note: No input will result in using the default AWS dir (~/.aws/)"
    read -r aws_env

    if [ -z "$aws_env" ]; then
        aws_env="~/.aws/"
    fi
    # to get proper tilde expansion
    aws_env="${aws_env/#\~/$HOME}"
    if [[ -d ${aws_env} ]]; then # Directory that contains `credentials` and `config` files.
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

        # TODO: The following should happen only for AWS. This is because
        # in the yaml templates this field is hardcoded. Can be solved if
        # templating engine like helm, kustomize is used.
        if kubectl get secrets | grep aws > /dev/null 2>&1; then
            kubectl delete secret aws-credentials
        fi
        kubectl create secret generic aws-credentials --from-file=$LOCAL_AWS
    elif [[ -f $aws_env ]]; then # File contains env variables.
        source ${aws_env}
        cat > ${LOCAL_AWS} <<- EOM
[default]
aws_access_key_id = ${AWS_ACCESS_KEY_ID}
aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}
region = ${AWS_REGION}
output = json
EOM

        if kubectl get secret | grep aws > /dev/null 2>&1; then
            kubectl delete secret aws-credentials
        fi
        kubectl create secret generic aws-credentials --from-file=${LOCAL_AWS}
    else
      echo "File nor directory exists under: ${aws_env}"
    fi
fi

rm -rf ${LOCAL_AWS}
