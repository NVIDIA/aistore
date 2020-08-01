## github_release.sh
This is a script for adding release assets to an existing github release of `AIStore`
given a release tag. Currently it adds the **ais**, **aisfs** and **aisloader** binaries alongwith their sha256 checksum to the github release.

### Usage

- [Create](https://docs.github.com/en/github/administering-a-repository/managing-releases-in-a-repository) a release on "github.com/NVIDIA/aistore", and note the tag
- Have the [GITHUB_OAUTH_TOKEN](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) and the [GITHUB_RELEASE_TAG](https://git-scm.com/book/en/v2/Git-Basics-Tagging) ready.

Command
```console
$ GITHUB_OAUTH_TOKEN=<oauth token> GITHUB_RELEASE_TAG=<release tag> ./github_release.sh
```
