FROM docker.io/golang:1.22 AS builder

ARG mode
ARG providers

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN mkdir -p /go/src/github.com/NVIDIA/aistore
WORKDIR /go/src/github.com/NVIDIA/aistore
COPY . .

RUN MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"} make node

FROM docker.io/ubuntu:22.04

ARG MODE
RUN apt-get clean && apt-get update &&\
  set -eux &&\
  apt-get --no-install-recommends -y install \
      curl \
      git \
      sysstat \
      attr \
      lsof \
      ca-certificates \
      util-linux \
      coreutils \
      make &&\
  apt-get -y clean all

COPY deploy/dev/local/aisnode_config.sh /etc/ais/aisnode_config.sh
COPY deploy/conf/limits.conf /etc/security/limits.conf

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

# Copy over the binaries from the previous stage.
COPY --from=builder /go/bin /go/bin
COPY --from=builder /usr/local/go/bin /usr/local/go/bin

RUN mkdir -p /etc/ais
RUN mkdir /usr/nvidia

EXPOSE 8080
