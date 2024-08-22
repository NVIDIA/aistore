#
# Dockerfile to build an aisnode Docker image
#

ARG INSTALLER_IMAGE=debian:bookworm
ARG BUILDER_IMAGE=golang:1.22
ARG BASE_IMAGE=gcr.io/distroless/base-debian12


FROM --platform=${TARGETPLATFORM:-linux/amd64} ${INSTALLER_IMAGE} as installer

RUN apt-get update -yq && apt-get install -y --no-install-recommends \
    ca-certificates


FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/aistore

COPY . .

ARG mode
ARG providers
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"} \
    make node


FROM --platform=${TARGETPLATFORM:-linux/amd64} ${BASE_IMAGE} as base

COPY --from=installer /etc/ssl/certs /etc/ssl/certs

COPY --from=builder /go/bin/aisnode /usr/bin/aisnode

ENTRYPOINT ["aisnode"]
