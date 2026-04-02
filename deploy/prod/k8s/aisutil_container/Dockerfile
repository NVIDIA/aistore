# Dockerfile to build an AIS Util Docker image

ARG BUILDER_IMAGE=docker.io/library/golang:1.26
ARG BASE_IMAGE=docker.io/library/debian:trixie-slim

FROM --platform=${BUILDPLATFORM:-linux/amd64} ${BUILDER_IMAGE} AS builder

WORKDIR /go/src/aistore

COPY . .

ARG version
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} VERSION=${version} make cli xmeta aisloader

RUN mv cmd/cli/autocomplete /tmp/autocomplete

FROM ${BASE_IMAGE}

ARG TARGETARCH
ARG KUBECTL_VERSION=v1.35.3

RUN apt-get update -yq && \
    apt-get install -y python3 python3-pip python3-venv && \
    apt-get install -y wget sysstat curl git iputils-ping make coreutils net-tools iproute2 tcptrack vim \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /venv \
    && /venv/bin/pip install --upgrade pip \
    && /venv/bin/pip install --no-cache-dir aistore
ENV PATH="/venv/bin:$PATH"

COPY --from=builder /go/bin /usr/bin/

RUN KUBECTL_URL="https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" && \
    curl -fSL "${KUBECTL_URL}" -o /tmp/kubectl && \
    curl -fSL "${KUBECTL_URL}.sha256" -o /tmp/kubectl.sha256 && \
    echo "$(cat /tmp/kubectl.sha256)  /tmp/kubectl" | sha256sum -c - && \
    install -m 0755 /tmp/kubectl /usr/local/bin/kubectl && \
    rm -f /tmp/kubectl /tmp/kubectl.sha256

# Install autocomplete
COPY --from=builder /tmp/autocomplete /tmp/autocomplete
RUN /tmp/autocomplete/install.sh && \
    echo "source /tmp/autocomplete/bash" >> /etc/bash.bashrc

WORKDIR /

CMD ["tail", "-f", "/dev/null"]
