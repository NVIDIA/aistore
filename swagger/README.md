## How to generate client code?

1. Obtain swagger-codegen jar by running the following command:
    ```
    wget http://central.maven.org/maven2/org/openapitools/openapi-generator-cli/3.0.2/openapi-generator-cli-3.0.2.jar -O openapi-generator-cli.jar
    ```
   Source: [openapitools/openapi-generator](https://github.com/openapitools/openapi-generator)
   
2. Run the following commands:
    ```
    cd </path/to/dfcpub>
    java -jar </path/to/openapi-generator-cli.jar> generate -i swagger/rest-api-specification.yaml -g python -o ./python-client/
    ```
    
3. Run tests by running the following commands:
    ```
    BUCKET=<bucket_name> python -m unittest discover python-client/
    ```
