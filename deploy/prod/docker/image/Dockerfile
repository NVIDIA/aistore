ARG BUILDER_IMAGE=docker.io/library/golang:1.26
ARG BASE_IMAGE=docker.io/library/alpine:latest

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/aistore

COPY . .

ARG mode
ARG providers
ARG version
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    CGO_ENABLED=0 VERSION=${version} MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp azure"} \
    make node

FROM ${BASE_IMAGE}

RUN apk upgrade --no-cache && apk add --no-cache \
    bash \
    sysstat \
    attr

WORKDIR /build
COPY deploy/prod/docker/image/entrypoint.sh /opt/entrypoint.sh
COPY deploy/dev/utils.sh ./
COPY deploy/dev/local/aisnode_config.sh ./
COPY deploy/conf/limits.conf /etc/security/limits.conf
COPY --from=builder /go/bin/aisnode bin/

ENTRYPOINT ["/bin/bash", "/opt/entrypoint.sh"]
