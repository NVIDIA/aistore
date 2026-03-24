COMPOSE     ?= $(shell command -v podman >/dev/null 2>&1 && echo "podman compose" || echo "docker compose")
IMAGE_REPO  ?= aistorage/cluster-minimal
IMAGE_TAG   ?= latest
IMAGE        = $(IMAGE_REPO):$(IMAGE_TAG)

.PHONY: build up up-build pull down logs status clean

# Build the image from source
build:
	IMAGE=$(IMAGE) $(COMPOSE) build

# Pull the pre-built image from the registry
pull:
	$(COMPOSE) pull

# Start the cluster (pulls image if not present)
up:
	IMAGE=$(IMAGE) $(COMPOSE) up -d --pull missing

# Build from source and start
up-build: build
	IMAGE=$(IMAGE) $(COMPOSE) up -d

# Stop the cluster
down:
	$(COMPOSE) down

# Stop and remove data volumes
clean:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

status:
	$(COMPOSE) ps
