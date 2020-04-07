---
layout: post
title: TAR2TF
permalink: deploy/tar2tf
redirect_from:
 - deploy/tar2tf/README.md/
---

## tar2tf Demo - Docker

tar2tf Docker deployment shows capabilities of tar2tf module.
Within a docker instance it creates ready to use setup to interact with tar2tf.

```console
$ ./docker/start.sh
```

This command will build and start Docker container, output logs to the current terminal window, deploy AIS cluster with
`tar-bucket` bucket, put necessary data and start Jupyter notebook server.  

To begin the demo, go to `localhost:8888` or to the link displayed by Jupyter in the console.
Go to `examples/in_memory_notebook.ipynb` and interact with it.

Please note that the first build might take a lot of time, as it has to fetch all necessary dependencies.
Subsequent builds will be much faster, thanks to docker caching.

To kill the docker, click Jupyter `Shut Down` button in the browser or send `kill` to the console.

### Datasets

By default, `gs://lpr-imagenet/imagenet_train-{0000..0002}.tgz` tars will be downloaded and uploaded to `tar-bucket`.

To use locally stored datasets, specify path to the directory in the command line with option `-v`.

```console
$ ./docker/start.sh -v=/home/user/dataset/
```
