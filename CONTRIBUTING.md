# Contributing to AIStore

The AIStore project repository follows an open source model where anyone is allowed and encouraged to contribute. However, contributing to AIStore has a few guidelines that must be followed.


## Contribution Workflow

The AIStore project repository maintains a contribution structure in which everyone *proposes* changes to the codebase via *pull requests*. To contribute to AIStore:

1. [Fork the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork),
2. [Create branch for issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-a-branch-for-an-issue),
3. [Test changes](#testing-changes),
4. [Format changes](#formatting-changes),
5. [Commit changes (w/ sign-off)](#signing-off-commits),
6. [Squash commits](#squashing-changes),
5. [Create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).


#### Formatting Changes

AIStore maintains a few formatting rules to ensure a consistent coding style. These rules are checked and enforced by `black`, `pylint`, `gofmt`, etc.  Before committing any changes, make sure to check (or fix) all changes against the formatting rules as follows:

```console
$ cd aistore

# Run linter on entire codebase
$ make lint

# Check code formatting
$ make fmt-check

# Fix code formatting
$ make fmt-fix

# Check for any misspelled words
$ make spell-check
```

> For more information, run `make help`.


#### Testing Changes

Before committing any changes, run the following tests to verify any added changes to the codebase:

```console
$ cd aistore

# Run short tests
$ BUCKET=tmp make test-short

# Run all tests
$ BUCKET=<existing-cloud-bucket> make test-long
```

To run Python-related tests:

```console
$ cd aistore/python

# Run all Python tests
$ make python_tests

# Run Python sdk tests
$ make python_sdk_tests

# Run Python ETL tests
$ make python_etl_tests

# Run Python botocore monkey patch tests
$ make python_botocore_tests
```


#### Signing-Off Commits

All contributors must *sign-off* on each commit. This certifies that each contribution is that contributor's original work per the following *Developer Certificate of Origin*[^developer-certificate-of-origin].

[^developer-certificate-of-origin]: **Developer Certificate of Origin**
    ```
    Developer Certificate of Origin
    Version 1.1

    Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
    1 Letterman Drive
    Suite D4700
    San Francisco, CA, 94129

    Everyone is permitted to copy and distribute verbatim copies of this
    license document, but changing it is not allowed.


    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
        have the right to submit it under the open source license
        indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.
    ```

Commits can be signed off by using the `git` command's `--signoff` (or `-s`) option:

```bash
$ git commit -s -m "Add new feature"
```

This will append the following type of footer to the commit message:

```
Signed-off-by: Your Name <your@email.com>
```

> **Note**: Commits that are not signed-off cannot be accepted or merged.


#### Squashing Commits

If a pull request contains more than one commit, [squash](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/about-pull-request-merges) all commits into one. 

The basic squashing workflow is as follows:

```console
git checkout <your-pr-branch>
git rebase -i HEAD~<# of commits to squash>
```


## Raise an Issue 

If a bug requires more attention, raise an issue [here](https://github.com/NVIDIA/aistore/issues). We will try to respond to the issue as soon as possible.

Please give the issue an appropriate title and include detailed information on the issue at hand.

---
