#
# Dockerfile to build an authn Docker image
#

ARG BUILDER_IMAGE=docker.io/library/golang:1.26
ARG BASE_IMAGE=gcr.io/distroless/base-debian12:nonroot

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/authn

COPY . .

ARG version
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} VERSION=${version} make authn

FROM ${BASE_IMAGE}

COPY --from=builder /go/bin/authn /usr/bin/authn

COPY ./deploy/prod/k8s/authn_container/authn-default.json /etc/ais/authn/authn.json

ENTRYPOINT ["authn", "-config=/etc/ais/authn"]
