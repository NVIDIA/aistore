# Contributing to AIStore

The AIStore project repository follows an open source model where anyone is allowed and encouraged to contribute. However, contributing to AIStore has a few guidelines that must be followed.

## Contents

- [AI-Assisted Contributions](#ai-assisted-contributions)
  - [Human Responsibility](#human-responsibility)
  - [Disclosure](#disclosure)
  - [Licensing and Sign-Off](#licensing-and-sign-off)
  - [Issues and Feature Requests](#issues-and-feature-requests)
- [Contribution Workflow](#contribution-workflow)
  - [Formatting Changes](#formatting-changes)
  - [Coding Style](#coding-style)
  - [Testing Changes](#testing-changes)
  - [Previewing Documentation Changes](#previewing-documentation-changes)
  - [Commit Messages](#commit-messages)
  - [Signing-Off Commits](#signing-off-commits)
  - [Squashing Commits](#squashing-commits)
- [Raise an Issue](#raise-an-issue)

## AI-Assisted Contributions

We welcome AI-assisted contributions. AI tools may assist with development,
but the human contributor remains responsible for the entire submission.
All existing contribution, testing, formatting, licensing, and sign-off
requirements apply.

### Human Responsibility

Submit only changes you have personally reviewed, understand, and can
explain. Verify generated code and tests, respond to review feedback, and
address bugs introduced by your contribution.

Do not submit pull requests or issue reports produced by unattended AI
scans without human investigation and validation. For bug fixes, provide
a reproducer, regression test, or other concrete evidence of the problem
and verify that the change addresses it. State any testing or verification
you could not complete.

### Disclosure

Disclose substantive AI assistance in the pull request description,
including assistance with code, tests, documentation, or identifying the
problem being addressed. Disclosure is required even if you subsequently
edited the generated content.

Include:

- The tool and model used, where known.
- Which parts of the contribution were assisted and how.
- How you reviewed and tested the result, including any limitations.

Routine autocomplete, spelling and grammar corrections, and mechanical
formatting do not require disclosure. When in doubt, disclose.

### Licensing and Sign-Off

You are responsible for ensuring that you have the right to submit the
contribution under the project's MIT license and any applicable file-level
licenses, consistent with the Developer Certificate of Origin.

Check that your AI tool's terms permit the intended contribution. Do not
submit copied third-party material unless its license permits inclusion
and all required notices and attribution are preserved.

Commit authorship and DCO sign-off must identify responsible humans.
Do not list an AI tool as an author, co-author, or signatory. AI assistance
disclosure does not replace your own Signed-off-by certification.

### Issues and Feature Requests

Describe the problem or proposal in your own words and verify the claims
you submit. AI may help you prepare a report, but do not submit raw,
unedited AI output or speculative findings you have not investigated.

Maintainers may request further explanation or verification, or close
contributions that do not meet these requirements without detailed review.


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

AIStore maintains a few formatting rules to ensure a consistent coding style. These rules are checked and enforced by `ruff`, `pylint`, `gofmt`, etc.  Before committing any changes, make sure to check (or fix) all changes against the formatting rules as follows:

Run `make lint` before submitting any commits. It must pass.

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


#### Coding Style

Follow the repository's existing coding style. Use the surrounding code and
established patterns in the package as your guide.

For comments above functions and methods:

- Do not repeat the function or method name.
- Usually start with a lowercase verb.
- Keep the summary brief; optionally follow it with brief bulleted details.

Keep critical sections short. Do not hold a mutex across a network request
of any kind. Avoid `nlog` calls while holding a mutex where possible, and keep
syscalls inside critical sections to the absolute minimum.

Arrange struct fields to minimize alignment padding, unless there is a
specific reason to group related fields together.


#### Testing Changes

When choosing between an integration test and a unit test, choose an
integration test that runs against a live cluster: a local playground or
minikube-based Kubernetes cluster.

Every bug fix must include an integration test that passes with the fix and
fails without it. Verify both outcomes.

For intermittent failures, repeat the test enough times to eliminate doubt
about the fix. Report the commands and run counts used to verify the failure
without the fix and the repeated passes with it.

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

#### Previewing Documentation Changes

The production website is still built from `docs/` with Jekyll and Netlify
(`netlify.toml` runs `bundle exec jekyll build` inside `docs/`). Fern is
available as a separate preview and validation surface for contributors.

To validate the Fern docs configuration without publishing:

```console
cd aistore
python3 -m pip install pyyaml
npm install -g fern-api@5.50.5
bash scripts/fern/generate-pages.sh --check
```

`make fern-check` is available as a convenience wrapper for the same check.

To run the local Fern preview:

```console
make fern-preview
```

`make fern-preview` serves the Fern docs at `http://localhost:3000`.
If `FERN_TOKEN` is not set, the local preview uses a stub Python API
reference page; set `FERN_TOKEN` before running the command to include the
generated Python API reference. Do not use `make fern-build` for local
checks; it publishes to Fern.

#### Commit Messages

Commit messages must precisely describe the changes being committed. Keep
the title and body consistent with the final diff, including the problem
addressed and the resulting behavior. Avoid claims the changes or validation
do not support.

Release notes are usually drafted from commit messages; their accuracy
matters beyond the individual review.

#### Signing-Off Commits

All contributors must *sign-off* on each commit. This certifies that the contributor has the right to submit the contribution under the applicable open-source license, as described in the *Developer Certificate of Origin*[^developer-certificate-of-origin] below.

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
