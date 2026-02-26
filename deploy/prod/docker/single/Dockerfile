ARG BUILDER_IMAGE=docker.io/library/golang:1.25
ARG BASE_IMAGE=docker.io/library/alpine:latest

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/aistore

COPY . .

ARG mode
ARG providers
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    CGO_ENABLED=0 MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"} \
    make node

FROM ${BASE_IMAGE}

ARG providers
ENV AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"}

RUN apk upgrade --no-cache && apk add --no-cache \
    bash \
    sysstat \
    attr

WORKDIR /build
COPY deploy/prod/docker/single/entrypoint entrypoint/
COPY deploy/dev/utils.sh ./
COPY deploy/dev/local/aisnode_config.sh ./
COPY deploy/conf/limits.conf /etc/security/limits.conf
COPY --from=builder /go/bin/aisnode bin/

EXPOSE 51080/tcp

ENTRYPOINT ["/bin/bash", "entrypoint/entrypoint.sh"]
