#
# Makefile wrapper to Docker builds
#
# Usage:
#
# Set tag in env var TAG_* as below and make -e for one of the targets.
#
# e.g., env TAG_AISNODE=23 make -e aisnode
#
# The tag value is managed externally, such as by the user on
# interactive use or as sourced from some state for a Jenkins build.
#
# Must be run from the directory in which this Makefile resides.
#
# Assumes docker is setup to run without sudo.
#

#
# We expect these to be provided by environment, so set them to
# invalid tag values here.
#
TAG_AISNODE = .must_set_in_environment
TAG_AISNODE_DEBUG = .must_set_in_environment
TAG_INITCONTAINER =  .must_set_in_environment

#
# These are most likely fixed, but can be over-ridden from env if required
#
NAME_AISNODE=aisnode
NAME_AISNODE_DEBUG=aisnode_debug
NAME_INITCONTAINER=ais-kubectl

#
# Repos to publish to, override from env if needed
#
REPO_AISNODE=quay.io/nvidia/aisnode
REPO_AISNODE_DEBUG=quay.io/nvidia/aisnode-debug
REPO_INITCONTAINER=quay.io/nvidia/aisnode-kubectl
REPO_TAG_AISNODE=$(TAG_AISNODE)
REPO_TAG_AISNODE_DEBUG=$(TAG_AISNODE_DEBUG)
REPO_TAG_INITCONTAINER=$(TAG_INITCONTAINER)

DOCKER=docker
BUILD_AISNODE=./build_ais_binary.sh

#
# Dockerfiles
#
DOCKERFILE_AISNODE = Dockerfile_aisnode
DOCKERFILE_AISNODE_DEBUG = Dockerfile_aisnode_debug
DOCKERFILE_INITCONTAINER = Dockerfile_initcontainer

#
# Docker start script.
#
DOCKER_START=ais_docker_start.sh
DOCKER_START_BASENAME:=$(shell basename $(DOCKER_START))

DOCKER_CTXDIR:=$(shell mktemp -d)

#
# Targets to build a container
#
aisnode_image:
	$(BUILD_AISNODE) $(DOCKER_CTXDIR)
	cp $(DOCKERFILE_AISNODE) $(DOCKER_CTXDIR)/Dockerfile
	cp $(DOCKER_START) $(DOCKER_CTXDIR) 
	cp ais_readiness.sh $(DOCKER_CTXDIR)
	git show-branch --more=10 > $(DOCKER_CTXDIR)/git-showbranch.out
	$(DOCKER) build \
		-f $(DOCKER_CTXDIR)/Dockerfile \
		-t $(NAME_AISNODE):$(TAG_AISNODE) \
		--build-arg start_script=$(DOCKER_START_BASENAME) \
		$(DOCKER_CTXDIR)
	rm -rf $(DOCKER_CTXDIR)

aisnode_debug_image:
	cp $(DOCKERFILE_AISNODE_DEBUG) $(DOCKER_CTXDIR)/Dockerfile
	cp $(DOCKER_START) $(DOCKER_CTXDIR)
	cp ais_readiness.sh $(DOCKER_CTXDIR)
	mkdir $(DOCKER_CTXDIR)/go
	cp -r $(HOME)/go $(DOCKER_CTXDIR)/go
	$(DOCKER) build \
		-f $(DOCKER_CTXDIR)/Dockerfile \
		-t $(NAME_AISNODE_DEBUG):$(TAG_AISNODE_DEBUG) \
		--build-arg start_script=$(DOCKER_START_BASENAME) \
		$(DOCKER_CTXDIR)
	rm -rf $(DOCKER_CTXDIR)

initcontainer_image:
	cp $(DOCKERFILE_INITCONTAINER) $(DOCKER_CTXDIR)/Dockerfile
	$(DOCKER) build \
		-f $(DOCKER_CTXDIR)/Dockerfile \
		-t $(NAME_INITCONTAINER):$(TAG_INITCONTAINER) \
		$(DOCKER_CTXDIR)
	rm -rf $(DOCKER_CTXDIR)

#
# Targets to tag and push an already-built image
#
push_aisnode:
	$(DOCKER) image tag $(NAME_AISNODE):$(TAG_AISNODE) $(REPO_AISNODE):$(REPO_TAG_AISNODE)
	$(DOCKER) push $(REPO_AISNODE):$(REPO_TAG_AISNODE)

push_aisnode_debug:
	$(DOCKER) image tag $(NAME_AISNODE_DEBUG):$(TAG_AISNODE_DEBUG) $(REPO_AISNODE_DEBUG):$(REPO_TAG_AISNODE_DEBUG)
	$(DOCKER) push $(REPO_AISNODE_DEBUG):$(REPO_TAG_AISNODE_DEBUG)

push_initcontainer:
	$(DOCKER) image tag $(NAME_INITCONTAINER):$(TAG_INITCONTAINER) $(REPO_INITCONTAINER):$(REPO_TAG_INITCONTAINER)
	$(DOCKER) push $(REPO_INITCONTAINER):$(REPO_TAG_INITCONTAINER)

#
# Build, tag & push container images
#
aisnode: aisnode_image push_aisnode

aisnode_debug: aisnode_debug_image push_aisnode_debug

initcontainer: initcontainer_image push_initcontainer