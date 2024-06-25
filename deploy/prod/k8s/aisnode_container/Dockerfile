#
# Dockerfile to build an aisnode Docker image
#

FROM ubuntu:22.04 as installer

RUN apt-get update -yq \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    git

# TODO -- FIXME: remove statsd
RUN cd /opt && git clone https://github.com/etsy/statsd.git && rm -rf statsd/.git


FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.22 AS builder

WORKDIR /go/src/aistore

COPY . .

ARG mode
ARG providers
ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"} \
    make node


FROM ubuntu:22.04 as base

# Copy over the binaries.
COPY --from=builder /go/bin /usr/bin/
COPY --from=installer /opt/statsd /opt
COPY --from=installer /etc/ssl /etc/ssl

ENTRYPOINT ["aisnode"]
