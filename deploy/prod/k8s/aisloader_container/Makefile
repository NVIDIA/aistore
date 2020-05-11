#
# Makefile wrapper for aisloader image builds
#
# Usage:
#
# env TAG_AISLOADER=... make -e aisloader
#
# The tag value is managed externally.
#

#
# An invalid default value in case not set in env or if make not run with -e
#
TAG_AISLOADER = .must_set_in_environment

#
# Image name
#
NAME_AISLOADER=aisloader

#
# Repo to publish to, override from env if needed
#
REPO_AISLOADER=quay.io/nvidia/aisloader
REPO_TAG_AISLOADER=$(TAG_AISLOADER)

DOCKER=docker
BUILD_AISLOADER=./build_aisloader_binary.sh
DOCKERFILE_AISLOADER=Dockerfile
DOCKER_START=aisloader_docker_start.sh
DOCKER_START_BASENAME=$(shell basename $(DOCKER_START))

DOCKER_CTXDIR:=$(shell mktemp -d)

aisloader_image:
	$(BUILD_AISLOADER) $(DOCKER_CTXDIR)/aisloader
	cp $(DOCKERFILE_AISLOADER) $(DOCKER_CTXDIR)/Dockerfile
	cp $(DOCKER_START) $(DOCKER_CTXDIR)
	git show-branch --more=10 > $(DOCKER_CTXDIR)/git-showbranch.out
	sudo $(DOCKER) build \
		-f $(DOCKER_CTXDIR)/Dockerfile \
		-t $(NAME_AISLOADER):$(TAG_AISLOADER) \
		--build-arg start_script=$(DOCKER_START_BASENAME) \
		$(DOCKER_CTXDIR)
	#rm -rf $(DOCKER_CTXDIR)

push_aisloader:
	sudo $(DOCKER) image tag $(NAME_AISLOADER):$(TAG_AISLOADER) $(REPO_AISLOADER):$(REPO_TAG_AISLOADER)
	sudo $(DOCKER) push $(REPO_AISLOADER):$(REPO_TAG_AISLOADER)

aisloader: aisloader_image push_aisloader