#
# Dockerfile to build an authn Docker image
#

ARG BUILDER_IMAGE=golang:1.22
ARG BASE_IMAGE=gcr.io/distroless/base-debian12

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/authn

COPY . .

RUN make authn

FROM --platform=${TARGETPLATFORM:-linux/amd64} ${BASE_IMAGE} as base

COPY --from=builder /go/bin/authn /usr/bin/authn

ENTRYPOINT ["authn", "-config=/etc/authn"]
