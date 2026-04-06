#
# Dockerfile to build an aisnode Docker image
#

ARG BUILDER_IMAGE=docker.io/library/golang:1.26
ARG BASE_IMAGE=gcr.io/distroless/base-debian12

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/aistore

COPY . .

ARG mode
ARG providers
ARG tags
ARG version
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    VERSION=${version} MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcpi oci"} TAGS=${tags:-""} \
    make node


FROM ${BASE_IMAGE}

COPY --from=builder /go/bin/aisnode /usr/bin/aisnode

ENTRYPOINT ["aisnode"]
