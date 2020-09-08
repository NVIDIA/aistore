# Building AIStore Containers

You will need both the `aisnode` and `ais-kubectl` container images
(the latter runs as an initContainer to the former). The `ais-kubectl`
container rarely ever changes, while the `aisnode` container delivers
the main AIStore binary and runtime support and can be updated often.

Correspondingly, you will require a container repository name and tag
for each - the defaults point to a private repository server on `quay.io`.

1. Have Go available in your `PATH` or at `/usr/local/go/bin` and at the appropriate version - see function `check_go_version` in `build_ais_binary.sh`
1. Clone this repo
1. `cd deploy/prod/k8s/aisnode_container`
1. Likely just once: `REPO_INITCONTAINER=repo.name/ais/aisnode-kubectl REPO_TAG_INITCONTAINER=1 make initcontainer` will build, tag and push a container image named `repo.name/ais/aidnoe-kubectl:1`. The repo (`repo.name` in the example) must be already created on a repository server of your choosing.
1. To update AIStore: `REPO_AISNODE=repo.name/ais/aisnode REPO_TAG_AISNODE=20200504 make aisnode` will build, tag and push a container image named `repo.name/ais/aisnode:20200504`
