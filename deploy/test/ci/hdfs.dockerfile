FROM ubuntu:16.04

ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:/usr/local/go/bin:${PATH}"
ENV GOLANG_VERSION="1.15"

RUN apt-get update -yq
RUN apt-get --no-install-recommends -y install lsb-release sudo default-jre default-jdk
RUN apt-get --no-install-recommends -y install curl git sysstat attr build-essential lsof fuse coreutils

# Install Go.
RUN mkdir -p "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz &&\
  tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null 2>&1 &&\
  rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz
