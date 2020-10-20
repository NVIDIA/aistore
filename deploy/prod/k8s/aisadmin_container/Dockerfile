#
# Dockerfile to build an AIS admin Docker image
#
FROM golang:1.15

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN apt-get update -yq
RUN apt-get install -y wget sysstat curl git iputils-ping netcat make coreutils
RUN apt-get install -y net-tools iproute2 tcptrack
RUN mkdir -p /usr/local/bin

RUN git clone https://github.com/NVIDIA/aistore.git && cd aistore && \
    make cli xmeta aisloader && \
    cd .. && rm -rf aistore

WORKDIR /

CMD tail -f /dev/null
