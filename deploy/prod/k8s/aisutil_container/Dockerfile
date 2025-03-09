# Dockerfile to build an AIS Util Docker image

FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.24 AS builder

WORKDIR /go/src/aistore

RUN git clone --depth 1 https://github.com/NVIDIA/aistore.git

WORKDIR /go/src/aistore/aistore

ARG TARGETOS
ARG TARGETARCH

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} make cli xmeta aisloader

RUN mv cmd/cli/autocomplete /tmp/autocomplete

FROM ubuntu:24.04 as base

RUN apt-get update -yq && \
    apt-get install -y wget sysstat curl git iputils-ping make coreutils net-tools iproute2 tcptrack vim \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy over the binaries
COPY --from=builder /go/bin /usr/bin/

# Install autocomplete
COPY --from=builder /tmp/autocomplete /tmp/autocomplete
RUN /tmp/autocomplete/install.sh && \
    echo "source /tmp/autocomplete/bash" >> /etc/bash.bashrc

WORKDIR /

CMD ["tail", "-f", "/dev/null"]
