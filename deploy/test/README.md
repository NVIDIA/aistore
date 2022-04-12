To support development and testing we include here a script to run
all tests inside a docker container. This way you are can rebuild your local
sources, change tests, and play with local development without worrying
that the changes you've made impact other tests or other deployments.

To start testing, run:

```console
$ ./docker/test.sh --name=your_container_name
```

This command will build and start Docker container and will output logs to
current terminal window. Note that this will start only non-cloud (local) bucket
tests.

**TIP:** It is good practice to name your container according to the feature you
were developing eg.: `new_feature`. This way you can start multiple testing
dockers and they will not interfere with each other. Moreover, you will be able
easily distinguish and switch between different Dockers.

## Clouds

You can also provide credentials to `AWS` or `GCP` and pass it to script:

```console
$ ./docker/test.sh --name=your_container_name --aws=~/.aws
```

This way testing will use given cloud in the backend (uniquely named cloud
bucket will be created automatically).

Because of Docker isolation you can safely run multiple instances of testing
containers (remember to uniquely name the containers).

```console
$ ./docker/test.sh --name=your_container_name_local & # run local
$ ./docker/test.sh --name=your_container_name_cloud --aws=~/.aws & # run cloud (aws)
```
