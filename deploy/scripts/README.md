---
layout: post
title: SCRIPTS
permalink: deploy/scripts
redirect_from:
 - deploy/scripts/README.md/
---

This folder is a loose collection of scripts used for development and production. Script names and brief descriptions follow below.

## github_release.sh

Given a release tag, add *release assets* to an existing (and tagged) github release at [AIStore releases](https://github.com/NVIDIA/aistore/releases).

Currently, this will build and [upload](https://github.com/actions/upload-release-asset) **ais**, **aisfs**, and **aisloader** binaries along with their respective sha256 checksums.

### Usage

Once a new AIS release is posted, run the following command:

Command
```console
$ GITHUB_OAUTH_TOKEN=<oauth token> GITHUB_RELEASE_TAG=<release tag> ./github_release.sh
```

> This will require [GITHUB_OAUTH_TOKEN](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) and, again, the corresponding [GITHUB_RELEASE_TAG](https://git-scm.com/book/en/v2/Git-Basics-Tagging).
> WARNING: Choose all the permission options while creating the token and then delete the token post the release.

## bench.sh

A tool to run and compare benchmarks between current (the latest) commit and a specified one.

Utilizes `go test` and `benchcmp` tools.

Is used in our [Makefile](/aistore/Makefile) for integration into CI pipeline.

### Usage

```console
$ ./bench.sh cmp --dir "<directory to search for tests>" --verbose --post-checkout "<post checkout commands to run>"
```

## clean_deploy.sh

Development-only. Performs several useful commands including shutdown of a locally deployed cluster, `cleanup`, etc.

### Usage

```console
$ ./clean_deploy.sh <directory used for $AIS_DIR>
```

## boostrap.sh

Used internally by almost all our [Makefile](/aistore/Makefile) commands.

## utils.sh

A collection of common `shell` functions.
