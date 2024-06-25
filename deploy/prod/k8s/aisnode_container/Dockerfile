#
# Dockerfile to build an aisnode Docker image
#

FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.22 AS builder

WORKDIR /go/src/aistore

COPY . .

ARG mode
ARG providers
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"} \
    make node cli xmeta


FROM ubuntu:22.04 as base

RUN apt-get update -yq && apt-get dist-upgrade -y
RUN apt-get install -y --no-install-recommends \
	sysstat \
	attr \
	git \
	make \
	coreutils \
	lsof \
	util-linux \
	bash \
	ca-certificates \
	curl

RUN cd /opt && git clone https://github.com/etsy/statsd.git

# Copy over the binaries.
COPY --from=builder /go/bin /usr/bin/

COPY deploy/prod/k8s/aisnode_container/ais_docker_start.sh /ais_docker_start.sh
RUN chmod a+x /ais_docker_start.sh

CMD ["bash", "-c", "/ais_docker_start.sh"]
