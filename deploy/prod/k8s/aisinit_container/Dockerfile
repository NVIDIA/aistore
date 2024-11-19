#
# Dockerfile to build an aisinit Docker image
#

ARG INSTALLER_IMAGE=debian:bookworm
ARG BUILDER_IMAGE=golang:1.23
ARG BASE_IMAGE=gcr.io/distroless/static:nonroot

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/aisinit

COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} make aisinit


FROM ${BASE_IMAGE}
COPY --from=builder /go/bin/aisinit /usr/bin

ENTRYPOINT ["aisinit"]
