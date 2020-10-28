#
# Dockerfile to build an AIS admin Docker image
#
FROM golang:1.15

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN git clone https://github.com/NVIDIA/aistore.git && cd aistore && \
    make cli xmeta aisloader && \
    cd .. && rm -rf aistore

FROM ubuntu:18.04

RUN apt-get update -yq
RUN apt-get install -y wget sysstat curl git iputils-ping netcat make coreutils
RUN apt-get install -y net-tools iproute2 tcptrack

RUN mkdir -p /usr/local/bin
ENV PATH="/usr/local/bin:${PATH}"
COPY --from=0 /go/bin /usr/local/bin/

WORKDIR /

CMD tail -f /dev/null
