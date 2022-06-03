#
# Dockerfile to build an aisnode Docker image
#

FROM golang:1.18 AS builder

ARG mode
ARG providers

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN mkdir -p /go/src/github.com/NVIDIA/aistore
WORKDIR /go/src/github.com/NVIDIA/aistore
COPY . .


RUN MODE=${mode} AIS_BACKEND_PROVIDERS=${providers:-"aws gcp"} make node cli xmeta

FROM ubuntu:18.04

RUN apt-get update -yq && apt-get dist-upgrade -y
RUN apt-get install -y wget sysstat curl nodejs git iputils-ping netcat make coreutils net-tools iproute2 tcptrack

RUN cd /opt && git clone https://github.com/etsy/statsd.git

RUN mkdir -p /usr/local/bin
ENV PATH="/usr/local/bin:${PATH}"

# Copy over the binaries.
COPY --from=builder /go/bin /usr/local/bin/

COPY deploy/prod/k8s/aisnode_container/ais_docker_start.sh /ais_docker_start.sh
COPY deploy/prod/k8s/aisnode_container/ais_readiness.sh /ais_readiness.sh
RUN chmod a+x /ais_docker_start.sh /ais_readiness.sh

CMD ["bash", "-c", "/ais_docker_start.sh"]
