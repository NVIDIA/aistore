#!/bin/bash -uex
# Generate the aistore python api-client package

java --version ||\
    sudo apt install default-jre-headless

stat -t openapi-generator-cli.jar ||\
    wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/4.2.3/openapi-generator-cli-4.2.3.jar -O openapi-generator-cli.jar

java -jar openapi-generator-cli.jar generate -i openapi/openapi.yaml -c openapi/config.json -g python -o ./python/api-client/
