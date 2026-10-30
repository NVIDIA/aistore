# GitHub release scripts

## gosum.sh

From a clean release branch, compute the root module's checksums before publishing
its tag:

```console
$ ./scripts/github-release/gosum.sh v1.5.2
```

The input must be `v1.N.N`, with the `v` prefix and without leading zeros.
The corresponding Git tag is `v1.N.N`. Nested modules (`cmd/cli`, `cmd/ishard`)
are not tagged.

The wrapper changes to the repository root and requires a clean checkout,
including staged and untracked files, a complete history, and no Git operation
in progress. It fetches `main` directly from NVIDIA/AIStore on GitHub and requires
it to be an ancestor of HEAD, with no merge commits above it. The tag must be
absent both locally and on GitHub. Network failures stop the command.

The module ZIP is built with `git archive`, which applies Git attributes. The
module proxy sees only the committed tree, so the wrapper rejects a non-empty
`.git/info/attributes` and any committed `.gitattributes`, and runs the helper
without system, global, or user attribute files and Git config.

The helper pins the revision to a commit and verifies the module declaration at
that commit. It hashes the module ZIP's file names and contents, plus `go.mod`
separately. Checksums reach stdout only after the wrapper confirms HEAD and the
clean checkout are unchanged.

For a coordinated release, finish all root-module changes before hashing the
root. Updating only `cmd/cli/` and `cmd/ishard/` afterward leaves the root
checksums unchanged, because Go excludes nested modules from the root ZIP.
Recheck the root checksums after committing the nested modules' dependency
update and before tagging the final commit.

`gosum.sh` fetches metadata and computes checksums; it does not edit module files,
commit, tag, or publish. `gosum.go` can also be invoked directly for checksum
verification, without the wrapper's release checks:

```console
$ go run scripts/github-release/gosum.go . github.com/NVIDIA/aistore v1.5.1 refs/tags/v1.5.1
```

## upload-assets.sh

Given a release tag, add *release assets* to an existing (and tagged) github release at [AIStore releases](https://github.com/NVIDIA/aistore/releases).

Currently, this will build and [upload](https://github.com/actions/upload-release-asset) **ais** and **aisloader** binaries along with their respective sha256 checksums.

Once a new AIS release is posted, run the following command:

```console
$ GITHUB_OWNER="NVIDIA" GITHUB_REPO="aistore" GITHUB_OAUTH_TOKEN=<oauth token> GITHUB_RELEASE_TAG=<release tag> ./scripts/github-release/upload-assets.sh
```

Add `--replace` to overwrite assets that already exist on the release.

> This will require the [GitHub CLI](https://cli.github.com/) (`gh`).
> It will also require [GITHUB_OAUTH_TOKEN](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) and the corresponding [GITHUB_RELEASE_TAG](https://git-scm.com/book/en/v2/Git-Basics-Tagging).
> WARNING: Choose all the permission options while creating the token and then delete the token post the release.
