#
# Dockerfile to build an AIS admin Docker image
#

FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.22 AS builder

WORKDIR /go/src/aistore

COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    git clone https://github.com/NVIDIA/aistore.git && cd aistore && \
    make cli xmeta aisloader && \
    mv cmd/cli/autocomplete /tmp/autocomplete && \
    cd .. && rm -rf aistore


FROM ubuntu:22.04 as base

RUN apt-get update -yq
RUN apt-get install -y wget sysstat curl git iputils-ping netcat make coreutils net-tools iproute2 tcptrack vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy over the binaries.
COPY --from=builder /go/bin /usr/bin/

# Install autocomplete.
COPY --from=builder /tmp/autocomplete /tmp/autocomplete
RUN /tmp/autocomplete/install.sh && echo "source /tmp/autocomplete/bash" >> ~/.bashrc

WORKDIR /

CMD tail -f /dev/null
