## How to generate client code?

1. Obtain swagger-codegen jar by running the following command:
    ```shell
    wget http://central.maven.org/maven2/org/openapitools/openapi-generator-cli/3.3.2/openapi-generator-cli-3.3.2.jar -O openapi-generator-cli.jar
    ```
   Source: [openapitools/openapi-generator](https://github.com/openapitools/openapi-generator)

2. Run the following commands:
    ```shell
    cd </path/to/dfcpub>
    java -jar </path/to/openapi-generator-cli.jar> generate -i swagger/rest-api-specification.yaml -g python -o ./python-client/
    ```

3. Install `pip` - a package management system used to install and manage software packages written in Python. Visit the [installation page](https://pip.pypa.io/en/stable/installing/) for instructions on how to install `pip`.

4. Install required Python packages using `pip` and requirement files located in `python-client` directory:
    ```shell
    pip install -r python-client/requirements.txt
    pip install -r python-client/test-requirements.txt
    ```
   If you don't wish to install these packages system wide for a particular user, consider using a [virtual environment](https://pypi.org/project/virtualenv/).

## How to run tests?

Run tests by running the following command:
```shell
BUCKET=<bucket_name> python -m unittest discover python-client/
```
